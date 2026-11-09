"""_upstream_headers 純函式單元測試(不需伺服器)。"""
from aiohttp.test_utils import make_mocked_request

import gateway


def test_subscription_passthrough_keeps_auth():
    req = make_mocked_request("POST", "/v1/messages", headers={
        "Authorization": "Bearer oauth-tok",
        "anthropic-beta": "oauth-2025-04-20,other-beta",
        "x-custom": "1",
        "Host": "127.0.0.1:8788",
    })
    h = gateway._upstream_headers(req, "subscription")
    # 原封轉發驗證標頭
    assert h["Authorization"] == "Bearer oauth-tok"
    assert h["anthropic-beta"] == "oauth-2025-04-20,other-beta"
    assert h["x-custom"] == "1"
    # hop-by-hop(host)被剝除
    assert "host" not in {k.lower() for k in h}


def test_apikey_injects_key_and_strips_oauth(monkeypatch):
    monkeypatch.setattr(gateway, "ANTHROPIC_API_KEY", "sk-test-123")
    req = make_mocked_request("POST", "/v1/messages", headers={
        "Authorization": "Bearer oauth-tok",
        "anthropic-beta": "oauth-2025-04-20,fine-grained-tool-streaming-2025",
    })
    h = gateway._upstream_headers(req, "apikey")
    # 換 key:移除 OAuth Authorization、注入 x-api-key
    assert "Authorization" not in h
    assert h["x-api-key"] == "sk-test-123"
    assert h.get("anthropic-version") == gateway.ANTHROPIC_VERSION
    # oauth 專用 beta 被濾掉,其它保留
    assert "oauth" not in h.get("anthropic-beta", "")
    assert "fine-grained-tool-streaming-2025" in h["anthropic-beta"]


def test_apikey_drops_beta_when_only_oauth(monkeypatch):
    monkeypatch.setattr(gateway, "ANTHROPIC_API_KEY", "sk-test-123")
    req = make_mocked_request("POST", "/v1/messages", headers={
        "anthropic-beta": "oauth-2025-04-20",
    })
    h = gateway._upstream_headers(req, "apikey")
    # 濾掉唯一的 oauth beta 後,整個標頭移除
    assert "anthropic-beta" not in h


def test_apikey_preserves_existing_version(monkeypatch):
    monkeypatch.setattr(gateway, "ANTHROPIC_API_KEY", "sk-test-123")
    req = make_mocked_request("POST", "/v1/messages", headers={
        "anthropic-version": "2099-01-01",
    })
    h = gateway._upstream_headers(req, "apikey")
    # 呼叫端已帶 version 就不覆蓋
    assert h["anthropic-version"] == "2099-01-01"
