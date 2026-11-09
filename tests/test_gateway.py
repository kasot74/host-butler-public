"""整合測試:對著 mock 上游跑真實請求,驗證轉發 / 通道 / 串流行為。"""
import gzip
import json

import pytest
from aiohttp import web

import gateway


async def _mock_handler(request):
    body = await request.read()
    seen = {
        "path": request.raw_path,
        "method": request.method,
        "authorization": request.headers.get("Authorization"),
        "x_api_key": request.headers.get("x-api-key"),
        "anthropic_version": request.headers.get("anthropic-version"),
        "anthropic_beta": request.headers.get("anthropic-beta"),
        "body": body.decode() if body else None,
        # 這兩個 gateway 控制標頭不該外洩到上游
        "x_butler_public": request.headers.get("x-butler-public"),
        "x_gw_channel": request.headers.get("x-gw-channel"),
    }
    if request.path.endswith("/stream"):
        resp = web.StreamResponse(status=200, headers={"content-type": "text/event-stream"})
        await resp.prepare(request)
        for i in range(3):
            await resp.write(f"data: {i}\n\n".encode())
        await resp.write_eof()
        return resp
    # 模擬 Anthropic 對真實 /v1/messages 回應時附的真實用量標頭
    resp_headers = {}
    if request.path.endswith("/v1/messages"):
        resp_headers = {
            "anthropic-ratelimit-unified-5h-utilization": "0.42",
            "anthropic-ratelimit-unified-7d-utilization": "0.5",
        }
    return web.json_response({"seen": seen}, headers=resp_headers)


@pytest.fixture
async def mock_upstream(aiohttp_server):
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", _mock_handler)
    return await aiohttp_server(app)


@pytest.fixture(autouse=True)
def _reset_usage_cache():
    """_usage_cache 是 gateway 模組層級的全域狀態(單一 process 才需要這樣設計),
    測試間必須重置,否則前一個測試留下的快取會讓後面的測試斷言變得順序相依。"""
    gateway._usage_cache.clear()
    gateway._usage_cache_at = None
    yield


@pytest.fixture
async def client(aiohttp_client, mock_upstream, monkeypatch):
    base = str(mock_upstream.make_url("")).rstrip("/")
    monkeypatch.setattr(gateway, "UPSTREAM_BASE", base)
    monkeypatch.setattr(gateway, "UPSTREAM_PROXY", None)
    monkeypatch.setattr(gateway, "DEFAULT_CHANNEL", "subscription")
    return await aiohttp_client(gateway.build_app())


async def test_healthz(client):
    r = await client.get("/healthz")
    assert r.status == 200
    data = await r.json()
    assert data["ok"] is True
    assert data["default_channel"] == "subscription"


async def test_usage_no_data_before_any_request(client):
    r = await client.get("/usage")
    assert r.status == 200
    data = await r.json()
    assert data["available"] is False


async def test_usage_reflects_real_ratelimit_headers_after_request(client):
    await client.post("/v1/messages", data=b'{"model":"x"}')
    r = await client.get("/usage")
    data = await r.json()
    assert data["available"] is True
    assert data["headers"]["anthropic-ratelimit-unified-5h-utilization"] == "0.42"
    assert data["headers"]["anthropic-ratelimit-unified-7d-utilization"] == "0.5"


async def test_usage_public_requires_token(client, monkeypatch):
    """/usage 跟其他端點一樣:經 nginx /butler/ 進來的一律要合法虛擬金鑰。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    await client.post("/v1/messages", data=b'{"model":"x"}')
    r = await client.get("/usage", headers={"x-butler-public": "1"})
    assert r.status == 401
    r2 = await client.get("/usage", headers={"x-butler-public": "1", "x-api-key": "vk-secret"})
    assert r2.status == 200


async def test_unknown_channel_400(client):
    r = await client.post("/v1/messages", headers={"x-gw-channel": "bogus"}, data=b"{}")
    assert r.status == 400


async def test_apikey_without_key_502(client, monkeypatch):
    monkeypatch.setattr(gateway, "ANTHROPIC_API_KEY", "")
    r = await client.post("/v1/messages", headers={"x-gw-channel": "apikey"}, data=b'{"model":"x"}')
    assert r.status == 502


async def test_subscription_passthrough(client):
    r = await client.post("/v1/messages", headers={"Authorization": "Bearer tok"},
                          data=b'{"model":"claude-x"}')
    assert r.status == 200
    seen = (await r.json())["seen"]
    assert seen["authorization"] == "Bearer tok"   # 原封轉發
    assert seen["x_api_key"] is None
    assert seen["path"] == "/v1/messages"
    assert seen["body"] == '{"model":"claude-x"}'   # body 完整轉發


async def test_apikey_injection(client, monkeypatch):
    monkeypatch.setattr(gateway, "ANTHROPIC_API_KEY", "sk-test-xyz")
    r = await client.post("/v1/messages",
                          headers={"x-gw-channel": "apikey", "Authorization": "Bearer tok"},
                          data=b'{"model":"x"}')
    assert r.status == 200
    seen = (await r.json())["seen"]
    assert seen["x_api_key"] == "sk-test-xyz"       # gateway 注入
    assert seen["authorization"] is None            # 訂閱憑證被剝除
    assert seen["anthropic_version"] == gateway.ANTHROPIC_VERSION


async def test_streaming_passthrough(client):
    r = await client.post("/v1/stream", data=b"{}")
    assert r.status == 200
    text = await r.text()
    assert "data: 0" in text and "data: 1" in text and "data: 2" in text


def test_client_ip_prefers_x_real_ip_header():
    class _Req:
        headers = {"X-Real-IP": "203.0.113.7"}
        remote = "172.18.0.5"  # nginx container IP, not the real client
    assert gateway._client_ip(_Req()) == "203.0.113.7"


def test_client_ip_falls_back_to_remote_without_header():
    class _Req:
        headers = {}
        remote = "127.0.0.1"
    assert gateway._client_ip(_Req()) == "127.0.0.1"


async def test_log_entry_includes_client_ip(client, caplog):
    with caplog.at_level("INFO", logger="host-butler"):
        r = await client.post("/v1/messages", headers={"X-Real-IP": "198.51.100.9"},
                              data=b'{"model":"x"}')
    assert r.status == 200
    entries = [json.loads(rec.message) for rec in caplog.records if rec.message.startswith("{")]
    assert any(e.get("ip") == "198.51.100.9" for e in entries)


async def test_query_string_preserved(client):
    r = await client.post("/v1/messages?beta=true&x=1", data=b"{}")
    seen = (await r.json())["seen"]
    assert "beta=true" in seen["path"] and "x=1" in seen["path"]


async def test_channel_header_overrides_default(client, monkeypatch):
    # 預設 subscription,但用 header 覆寫成 apikey
    monkeypatch.setattr(gateway, "ANTHROPIC_API_KEY", "sk-override")
    r = await client.post("/v1/messages",
                          headers={"x-gw-channel": "apikey", "Authorization": "Bearer tok"},
                          data=b"{}")
    seen = (await r.json())["seen"]
    assert seen["x_api_key"] == "sk-override"


# ---- 對外(nginx /butler/)虛擬金鑰驗證 --------------------------------------

async def test_head_preflight_ok_without_key(client, monkeypatch):
    """Claude Code 的 HEAD 連通性檢查:不帶金鑰也放行 200(不轉發上游)。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    r = await client.head("/v1/messages", headers={"x-butler-public": "1"})
    assert r.status == 200


async def test_root_get_ok_without_key(client, monkeypatch):
    """根路徑 GET 連通性檢查:不帶金鑰也放行 200。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    r = await client.get("/", headers={"x-butler-public": "1"})
    assert r.status == 200


async def test_post_messages_still_requires_key(client, monkeypatch):
    """放行 preflight 不影響真正的 API:POST /v1/messages 無金鑰仍 401。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    r = await client.post("/v1/messages", headers={"x-butler-public": "1"}, data=b"{}")
    assert r.status == 401


async def test_public_no_token_401(client, monkeypatch):
    """帶 x-butler-public 但無金鑰 → 401,不轉發上游。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    r = await client.post("/v1/messages", headers={"x-butler-public": "1"}, data=b"{}")
    assert r.status == 401


async def test_public_wrong_token_401(client, monkeypatch):
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    r = await client.post("/v1/messages",
                          headers={"x-butler-public": "1", "x-api-key": "vk-wrong"}, data=b"{}")
    assert r.status == 401


async def test_public_valid_token_swaps_to_real_key(client, monkeypatch):
    """對外帶合法虛擬金鑰 → 強制 apikey 通道,換成真 key,虛擬金鑰不外洩。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    monkeypatch.setattr(gateway, "ANTHROPIC_API_KEY", "sk-real")
    r = await client.post("/v1/messages",
                          headers={"x-butler-public": "1", "x-api-key": "vk-secret"},
                          data=b'{"model":"x"}')
    assert r.status == 200
    seen = (await r.json())["seen"]
    assert seen["x_api_key"] == "sk-real"          # 換成真 key
    assert seen["authorization"] is None
    assert seen["x_butler_public"] is None          # 控制標頭不外洩上游
    assert seen["x_gw_channel"] is None


async def test_public_bearer_token_accepted(client, monkeypatch):
    """對外裝置也可把虛擬金鑰放 Authorization: Bearer。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    monkeypatch.setattr(gateway, "ANTHROPIC_API_KEY", "sk-real")
    r = await client.post("/v1/messages",
                          headers={"x-butler-public": "1", "Authorization": "Bearer vk-secret"},
                          data=b"{}")
    assert r.status == 200
    assert (await r.json())["seen"]["x_api_key"] == "sk-real"


async def test_public_ignores_subscription_override(client, monkeypatch):
    """對外請求即使指定 x-gw-channel=subscription 也一律走 apikey(不得透傳他人憑證)。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    monkeypatch.setattr(gateway, "ANTHROPIC_API_KEY", "sk-real")
    r = await client.post("/v1/messages",
                          headers={"x-butler-public": "1", "x-api-key": "vk-secret",
                                   "x-gw-channel": "subscription", "Authorization": "Bearer stolen"},
                          data=b"{}")
    seen = (await r.json())["seen"]
    assert seen["x_api_key"] == "sk-real"
    assert seen["authorization"] is None            # 帶來的 Bearer 被剝除


async def test_local_still_works_without_token(client, monkeypatch):
    """本機 loopback(無 x-butler-public)不受影響,免金鑰、預設 subscription。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    r = await client.post("/v1/messages", headers={"Authorization": "Bearer oauth"}, data=b"{}")
    assert r.status == 200
    seen = (await r.json())["seen"]
    assert seen["authorization"] == "Bearer oauth"  # 訂閱透傳照舊


# ---- 實驗性訂閱中繼(PUBLIC_CHANNEL=subscription)-----------------------------

async def test_public_subscription_relay_injects_host_oauth(client, monkeypatch):
    """對外走訂閱中繼:注入主機 OAuth、補 oauth beta,外部帶來的憑證被取代。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    monkeypatch.setattr(gateway, "PUBLIC_CHANNEL", "subscription")
    monkeypatch.setattr(gateway, "_host_oauth_token", lambda: "host-oauth-xyz")
    r = await client.post("/v1/messages",
                          headers={"x-butler-public": "1", "x-api-key": "vk-secret",
                                   "Authorization": "Bearer stolen"},
                          data=b'{"model":"x"}')
    assert r.status == 200
    seen = (await r.json())["seen"]
    assert seen["authorization"] == "Bearer host-oauth-xyz"  # 注入主機 OAuth
    assert seen["x_api_key"] is None                          # 不走 api key
    assert gateway.OAUTH_BETA in (seen["anthropic_beta"] or "")  # 補上 oauth beta


async def test_public_subscription_relay_unavailable_502(client, monkeypatch):
    """訂閱中繼但主機 OAuth 讀不到 → 502(不轉發)。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    monkeypatch.setattr(gateway, "PUBLIC_CHANNEL", "subscription")
    monkeypatch.setattr(gateway, "_host_oauth_token", lambda: None)
    r = await client.post("/v1/messages",
                          headers={"x-butler-public": "1", "x-api-key": "vk-secret"}, data=b"{}")
    assert r.status == 502


async def test_public_subscription_relay_still_requires_valid_key(client, monkeypatch):
    """訂閱中繼模式下,無合法虛擬金鑰仍 401(驗證先於中繼)。"""
    monkeypatch.setattr(gateway, "GW_TOKENS", {"vk-secret"})
    monkeypatch.setattr(gateway, "PUBLIC_CHANNEL", "subscription")
    monkeypatch.setattr(gateway, "_host_oauth_token", lambda: "host-oauth-xyz")
    r = await client.post("/v1/messages",
                          headers={"x-butler-public": "1", "x-api-key": "vk-wrong"}, data=b"{}")
    assert r.status == 401


def test_host_oauth_token_reads_credentials(tmp_path, monkeypatch):
    """_host_oauth_token 正確解析 claudeAiOauth.accessToken;每次讀檔取最新。"""
    cred = tmp_path / ".credentials.json"
    cred.write_text('{"claudeAiOauth":{"accessToken":"tok-1","refreshToken":"r"}}')
    monkeypatch.setattr(gateway, "OAUTH_CREDENTIALS_FILE", str(cred))
    assert gateway._host_oauth_token() == "tok-1"
    cred.write_text('{"claudeAiOauth":{"accessToken":"tok-2"}}')  # 模擬刷新
    assert gateway._host_oauth_token() == "tok-2"
    monkeypatch.setattr(gateway, "OAUTH_CREDENTIALS_FILE", "")
    assert gateway._host_oauth_token() is None


# ---- 真實 token 用量記錄(取代 client 端估算金額)-----------------------------

def test_extract_usage_plain_json():
    body = json.dumps({"usage": {"input_tokens": 10, "output_tokens": 5}}).encode()
    usage = gateway._extract_usage(body, {"Content-Type": "application/json"})
    assert usage == {"input_tokens": 10, "output_tokens": 5}


def test_extract_usage_sse_stream():
    """SSE 用量分散在 message_start(input_tokens)與最後一筆 message_delta
    (累計 output_tokens)兩種事件裡。"""
    body = (
        'data: {"type":"message_start","message":{"usage":{"input_tokens":12,"output_tokens":1}}}\n\n'
        'data: {"type":"message_delta","delta":{},"usage":{"output_tokens":34}}\n\n'
        'data: {"type":"message_stop"}\n\n'
    ).encode()
    usage = gateway._extract_usage(body, {"Content-Type": "text/event-stream"})
    assert usage == {"input_tokens": 12, "output_tokens": 34}


def test_extract_usage_gzip_encoded():
    raw = json.dumps({"usage": {"input_tokens": 3, "output_tokens": 4}}).encode()
    compressed = gzip.compress(raw)
    usage = gateway._extract_usage(
        compressed, {"Content-Type": "application/json", "Content-Encoding": "gzip"})
    assert usage == {"input_tokens": 3, "output_tokens": 4}


def test_extract_usage_unparseable_returns_none():
    assert gateway._extract_usage(b"not json", {"Content-Type": "application/json"}) is None
    assert gateway._extract_usage(b"{}", {"Content-Type": "application/json"}) is None


# ---- 被限流時原樣透傳,不重試(見 CLAUDE.md 決策記錄)---------------------

async def test_rate_limit_429_passed_through_without_retry(aiohttp_client, aiohttp_server, monkeypatch):
    """Gateway 不再自動重試 429 -- 收到上游 429 就立刻原樣回給 client,
    只打一次上游。"""
    calls = {"n": 0}

    async def handler(request):
        calls["n"] += 1
        return web.json_response(
            {"type": "error", "error": {"type": "rate_limit_error"}},
            status=429, headers={"retry-after": "0.05"})

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    upstream = await aiohttp_server(app)

    monkeypatch.setattr(gateway, "UPSTREAM_BASE", str(upstream.make_url("")).rstrip("/"))
    monkeypatch.setattr(gateway, "UPSTREAM_PROXY", None)
    monkeypatch.setattr(gateway, "DEFAULT_CHANNEL", "subscription")

    client = await aiohttp_client(gateway.build_app())
    r = await client.post("/v1/messages", data=b'{"model":"x"}')
    assert r.status == 429
    assert calls["n"] == 1
