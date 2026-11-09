"""host-butler — 本機 Claude API Gateway(最小可行版)

一條綁在 loopback 的反向代理通道:CLI 把 ANTHROPIC_BASE_URL 指到這裡,
Gateway 依「通道(channel)」決定用哪種上游驗證,再轉發到 api.anthropic.com,並記錄每筆請求。

通道(可用請求標頭 `x-gw-channel` 覆寫,否則用 DEFAULT_CHANNEL):
  - subscription : 原封轉發 client 帶來的驗證(Claude Code 的訂閱 OAuth)。現況即可用。
  - apikey       : 移除 client 的 Authorization,改注入本機設定的 sk-ant- API key。

刻意保持最小:只做「轉發 + 通道切換 + 基本 log」。限流 / 多 key 輪詢等留待之後。
"""
from __future__ import annotations

import gzip
import hmac
import json
import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler

import aiohttp
from aiohttp import web

# ---- 設定(全部來自環境變數,方便容器化)----------------------------------
LISTEN_HOST = os.environ.get("LISTEN_HOST", "127.0.0.1")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8788"))
UPSTREAM_BASE = os.environ.get("UPSTREAM_BASE", "https://api.anthropic.com").rstrip("/")
DEFAULT_CHANNEL = os.environ.get("DEFAULT_CHANNEL", "subscription").lower()
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_VERSION = os.environ.get("ANTHROPIC_VERSION", "2023-06-01")
# 上游是否再走 Squid 之類的 forward proxy;留空 = 直連(本機/容器皆有對外出口)
UPSTREAM_PROXY = os.environ.get("UPSTREAM_PROXY", "").strip() or None
LOG_FILE = os.environ.get("LOG_FILE", os.path.join(os.path.dirname(__file__), "..", "logs", "gateway.log"))

# 對外(經 nginx /butler/)暴露時接受的「虛擬金鑰」;逗號分隔。
# 空集合 = 不接受任何對外請求(對外一律 401)。真正的 sk-ant key 只存在 ANTHROPIC_API_KEY。
GW_TOKENS = {t.strip() for t in os.environ.get("GW_TOKENS", "").split(",") if t.strip()}

# nginx 只在對外 location 設此標頭(值 "1")並覆寫 client 帶來的同名值,故 client 無法偽造成「本機」。
# 帶此標頭的請求 = 來自公網,必須帶合法虛擬金鑰且強制走 apikey 通道。
PUBLIC_HEADER = "x-butler-public"

# 逐跳(hop-by-hop)標頭:不可轉發,且 aiohttp 會自行重建
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
}

# gateway 專用控制標頭:只在 gateway 端消化,絕不轉發到上游
GW_HEADERS = {PUBLIC_HEADER, "x-gw-channel", "x-gw-auth"}

# 每次真實上游回應時,把 Anthropic 回的真實用量標頭(anthropic-ratelimit-unified-*)
# 快取在記憶體 -- 這是官方算的真實 5h/7d 額度使用率,不是 client 端用牌價估出來的金額。
# /usage 直接回傳這份快照,不必為了查詢而多打一次上游、多耗 token。
_usage_cache: dict[str, str] = {}
_usage_cache_at: float | None = None


def _capture_usage_headers(headers) -> None:
    global _usage_cache_at
    found = {k.lower(): v for k, v in headers.items() if k.lower().startswith("anthropic-ratelimit-")}
    if found:
        _usage_cache.clear()
        _usage_cache.update(found)
        _usage_cache_at = time.time()


def _decode_body(raw: bytes, headers) -> bytes:
    if (headers.get("Content-Encoding") or "").lower() == "gzip":
        try:
            return gzip.decompress(raw)
        except OSError:
            return raw
    return raw


def _extract_usage(raw: bytes, headers) -> dict | None:
    """Best-effort real input/output token usage from a /v1/messages response
    body, for cost-accounting logs (not the client-side list-price estimate).
    Handles both the plain JSON shape and the SSE streaming shape (usage is
    split across `message_start` and the final `message_delta` events).
    Returns None for anything that doesn't parse -- error responses, other
    endpoints, unexpected shapes -- this is a log enrichment, not a critical path."""
    content_type = (headers.get("Content-Type") or "").lower()
    try:
        body = _decode_body(raw, headers)
        if "text/event-stream" in content_type:
            input_tokens = None
            output_tokens = None
            for line in body.decode("utf-8", errors="replace").splitlines():
                if not line.startswith("data:"):
                    continue
                payload = line[len("data:"):].strip()
                if not payload or payload == "[DONE]":
                    continue
                event = json.loads(payload)
                usage = (event.get("message") or {}).get("usage") or event.get("usage")
                if not usage:
                    continue
                if usage.get("input_tokens") is not None:
                    input_tokens = usage["input_tokens"]
                if usage.get("output_tokens") is not None:
                    output_tokens = usage["output_tokens"]
            if input_tokens is None and output_tokens is None:
                return None
            return {"input_tokens": input_tokens or 0, "output_tokens": output_tokens or 0}
        data = json.loads(body)
        usage = data.get("usage")
        if not usage:
            return None
        return {
            "input_tokens": usage.get("input_tokens", 0),
            "output_tokens": usage.get("output_tokens", 0),
        }
    except (ValueError, UnicodeDecodeError, AttributeError, TypeError):
        return None

# 對外(公網)驗證通過後走哪個通道:
#   apikey(預設、安全)= 換成本機真 sk-ant key。
#   subscription(實驗性「訂閱中繼」)= 注入本機 claude.ai OAuth,讓外部裝置騎主機的訂閱。
#     ⚠ 非官方支援、可能違反消費者條款、token 輪替時脆弱;僅供學習,詳見 README。
PUBLIC_CHANNEL = os.environ.get("PUBLIC_CHANNEL", "apikey").lower()
# 訂閱中繼用:容器內掛載的主機 OAuth 憑證檔(唯讀)。
# ⚠ 請掛「目錄」而非單檔——Claude Code 刷新 token 會原子替換此檔,單檔 bind-mount 會讀到舊 inode。
OAUTH_CREDENTIALS_FILE = os.environ.get("OAUTH_CREDENTIALS_FILE", "").strip()
# OAuth(訂閱)存取 messages 端點所需的 anthropic-beta 旗標
OAUTH_BETA = os.environ.get("OAUTH_BETA", "oauth-2025-04-20").strip()

# ---- logging:同時輸出到 stdout(docker logs)與檔案 ----------------------
logger = logging.getLogger("host-butler")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
logger.addHandler(_sh)
try:
    os.makedirs(os.path.dirname(os.path.abspath(LOG_FILE)), exist_ok=True)
    _fh = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5)
    _fh.setFormatter(_fmt)
    logger.addHandler(_fh)
except OSError as e:  # 檔案寫不了也不要讓服務起不來
    logger.warning("log file disabled: %s", e)


def _client_token(request: web.Request) -> str | None:
    """取出 client 帶來的憑證(對外裝置把虛擬金鑰放在 x-api-key 或 Bearer)。"""
    xk = request.headers.get("x-api-key")
    if xk:
        return xk.strip()
    auth = request.headers.get("authorization", "")
    if auth[:7].lower() == "bearer ":
        return auth[7:].strip()
    return None


def _token_ok(tok: str | None) -> bool:
    """常數時間比對虛擬金鑰。"""
    if not tok or not GW_TOKENS:
        return False
    return any(hmac.compare_digest(tok, t) for t in GW_TOKENS)


def _host_oauth_token() -> str | None:
    """讀取掛載進來的主機 claude.ai OAuth access token。

    每次讀檔(而非啟動時快取),以取得 Claude Code 刷新後的最新值。
    結構:{"claudeAiOauth": {"accessToken": "...", ...}}。
    """
    if not OAUTH_CREDENTIALS_FILE:
        return None
    try:
        with open(OAUTH_CREDENTIALS_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    oauth = data.get("claudeAiOauth") or data.get("oauth") or {}
    return (oauth.get("accessToken") or "").strip() or None


def _upstream_headers(request: web.Request, channel: str) -> dict[str, str]:
    """依 channel 產生要送往上游的標頭。"""
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in HOP_BY_HOP and k.lower() not in GW_HEADERS}

    if channel == "apikey":
        # 換 API key:移除訂閱 OAuth 相關的驗證,避免與 x-api-key 衝突
        for k in list(headers):
            lk = k.lower()
            if lk == "authorization":
                headers.pop(k)
            elif lk == "anthropic-beta":
                # 濾掉 oauth 專用的 beta 旗標,保留其它
                kept = [p.strip() for p in headers[k].split(",")
                        if p.strip() and "oauth" not in p.lower()]
                if kept:
                    headers[k] = ",".join(kept)
                else:
                    headers.pop(k)
        headers["x-api-key"] = ANTHROPIC_API_KEY
        headers.setdefault("anthropic-version", ANTHROPIC_VERSION)
    elif channel == "sub-relay":
        # 訂閱中繼:丟掉外部帶來的任何憑證,改注入本機的 claude.ai OAuth,
        # 並補上 OAuth 存取 messages 端點所需的 anthropic-beta 旗標。
        existing_beta = ""
        for k in list(headers):
            lk = k.lower()
            if lk in ("authorization", "x-api-key"):
                headers.pop(k)
            elif lk == "anthropic-beta":
                existing_beta = headers.pop(k)
        headers["authorization"] = f"Bearer {_host_oauth_token() or ''}"
        betas = [p.strip() for p in existing_beta.split(",") if p.strip()]
        if OAUTH_BETA and OAUTH_BETA not in betas:
            betas.append(OAUTH_BETA)
        if betas:
            headers["anthropic-beta"] = ",".join(betas)
        headers.setdefault("anthropic-version", ANTHROPIC_VERSION)
    # subscription:原封轉發,不動 client 的驗證標頭
    return headers


def _client_ip(request: web.Request) -> str:
    """來源 IP:公網請求走 nginx 的 X-Real-IP(見 nginx default.conf /butler/ location);
    本機 loopback 沒有這個標頭,退回 aiohttp 看到的 socket peer(即 127.0.0.1)。"""
    return request.headers.get("X-Real-IP") or request.remote or ""


async def handle(request: web.Request) -> web.StreamResponse:
    if request.path == "/healthz":
        return web.json_response({"ok": True, "default_channel": DEFAULT_CHANNEL,
                                  "public_auth": bool(GW_TOKENS),
                                  "public_channel": PUBLIC_CHANNEL})

    if request.path == "/usage" and request.method == "GET":
        # 同一套公網驗證:經 nginx /butler/ 進來的一律要合法虛擬金鑰;本機 loopback 直接放行。
        if request.headers.get(PUBLIC_HEADER) == "1" and not _token_ok(_client_token(request)):
            return web.json_response({"error": "unauthorized"}, status=401)
        if not _usage_cache:
            return web.json_response({"available": False, "reason": "no proxied request observed yet since startup"})
        return web.json_response({
            "available": True,
            "captured_at": _usage_cache_at,
            "headers": dict(_usage_cache),
        })

    # Claude Code 啟動時會對 base URL 做「連通性檢查」(HEAD,或 GET 根路徑),且不帶金鑰。
    # 若對此回 401,某些版本會判定「連不上」而不再送出真正的 API 請求。
    # 因此放行 preflight(不需金鑰、不轉發上游);真正的 API 呼叫(POST /v1/messages 等)
    # 仍會落到下面的驗證。
    if request.method == "HEAD" or request.path in ("", "/"):
        return web.Response(status=200)

    if request.headers.get(PUBLIC_HEADER) == "1":
        # 來自公網(nginx /butler/):必須帶合法虛擬金鑰。對外裝置永遠看不到真憑證,
        # 也無法用自己帶來的憑證透傳(下面會依 PUBLIC_CHANNEL 換掉)。
        if not _token_ok(_client_token(request)):
            return web.json_response({"error": "unauthorized"}, status=401)
        if PUBLIC_CHANNEL == "subscription":
            # 實驗性訂閱中繼:注入本機 OAuth(非官方支援,見 README/ROADMAP)。
            channel = "sub-relay"
            if not _host_oauth_token():
                return web.json_response(
                    {"error": "subscription relay 未就緒:主機 OAuth 憑證不可用"}, status=502)
        else:
            # 預設、安全:換成本機真 sk-ant key。
            channel = "apikey"
            if not ANTHROPIC_API_KEY:
                return web.json_response(
                    {"error": "gateway upstream key not configured"}, status=502)
    else:
        # 本機 loopback:維持原行為(預設 subscription,可用 x-gw-channel 覆寫),免金鑰。
        channel = request.headers.get("x-gw-channel", DEFAULT_CHANNEL).lower()
        if channel not in ("subscription", "apikey"):
            return web.json_response({"error": f"unknown channel: {channel}"}, status=400)
        if channel == "apikey" and not ANTHROPIC_API_KEY:
            return web.json_response(
                {"error": "apikey channel 尚未設定 ANTHROPIC_API_KEY"}, status=502)

    client_ip = _client_ip(request)
    body = await request.read()
    model = None
    if body:
        try:
            model = json.loads(body).get("model")
        except (ValueError, AttributeError):
            pass

    headers = _upstream_headers(request, channel)
    url = UPSTREAM_BASE + request.raw_path  # 含 query string
    started = time.monotonic()
    session: aiohttp.ClientSession = request.app["session"]

    try:
        async with session.request(
            request.method, url, headers=headers, data=body or None,
            proxy=UPSTREAM_PROXY, allow_redirects=False,
        ) as upstream:
            _capture_usage_headers(upstream.headers)
            resp = web.StreamResponse(status=upstream.status)
            # 排除 hop-by-hop,其餘原封保留(content-encoding 不在其中,故 gzip body 一致)
            for k, v in upstream.headers.items():
                if k.lower() not in HOP_BY_HOP:
                    resp.headers[k] = v
            await resp.prepare(request)
            raw_chunks = bytearray()
            async for chunk in upstream.content.iter_any():
                raw_chunks.extend(chunk)
                await resp.write(chunk)
            await resp.write_eof()
            status = upstream.status
            usage = _extract_usage(bytes(raw_chunks), upstream.headers)
    except aiohttp.ClientError as e:
        dur = (time.monotonic() - started) * 1000
        logger.error(json.dumps({
            "channel": channel, "method": request.method, "path": request.path,
            "model": model, "error": str(e), "dur_ms": round(dur, 1), "ip": client_ip,
        }, ensure_ascii=False))
        return web.json_response({"error": "upstream request failed", "detail": str(e)}, status=502)

    dur = (time.monotonic() - started) * 1000
    log_entry = {
        "channel": channel, "method": request.method, "path": request.path,
        "model": model, "status": status, "dur_ms": round(dur, 1),
        "req_bytes": len(body), "ip": client_ip,
    }
    if usage:
        log_entry["usage"] = usage
    logger.info(json.dumps(log_entry, ensure_ascii=False))
    return resp


async def _on_startup(app: web.Application) -> None:
    # auto_decompress=False:body 原封轉發,content-encoding 才不會與內容不一致
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_connect=30)
    app["session"] = aiohttp.ClientSession(auto_decompress=False, timeout=timeout)


async def _on_cleanup(app: web.Application) -> None:
    await app["session"].close()


def build_app() -> web.Application:
    app = web.Application(client_max_size=64 * 1024 * 1024)
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    app.router.add_route("*", "/{tail:.*}", handle)
    return app


def main() -> None:
    logger.info("host-butler starting on %s:%s → %s (default_channel=%s, public_channel=%s, "
                "upstream_proxy=%s, public_tokens=%d)",
                LISTEN_HOST, LISTEN_PORT, UPSTREAM_BASE, DEFAULT_CHANNEL, PUBLIC_CHANNEL,
                UPSTREAM_PROXY or "direct", len(GW_TOKENS))
    web.run_app(build_app(), host=LISTEN_HOST, port=LISTEN_PORT, print=None)


if __name__ == "__main__":
    main()
