"""
Tests for POST /api/v1/chat endpoint.

Covers the main scenarios a chat request can hit:
- Missing API key (should degrade gracefully with 503)
- Invalid input (empty question → 422 validation error)
- Successful query (mocked agent returns structured result)
- Agent failure (internal error → 500 with meaningful message)
"""
from unittest.mock import patch, AsyncMock


def test_chat_returns_503_without_api_key(client):
    """When the API key isn't configured, chat should return 503 instead of crashing."""
    with patch("api.routers.chat.settings") as mock_settings:
        mock_settings.anthropic_api_key = ""
        resp = client.post("/api/v1/chat", json={"question": "Which airline is best?"})
        assert resp.status_code == 503
        assert "ANTHROPIC_API_KEY" in resp.json()["detail"]


def test_chat_validates_empty_question(client):
    """Empty questions should be rejected at the validation layer."""
    resp = client.post("/api/v1/chat", json={"question": ""})
    assert resp.status_code == 422


def test_chat_success(client):
    """Happy path: agent processes the question and returns answer + tools used."""
    mock_result = {
        "answer": "Delta has the best on-time rate.",
        "tools_used": ["get_carrier_performance"],
    }
    with patch("api.routers.chat.settings") as mock_settings, \
         patch("api.routers.chat.run_agent", new_callable=AsyncMock, return_value=mock_result):
        mock_settings.anthropic_api_key = "test-key"
        resp = client.post("/api/v1/chat", json={"question": "Which airline is best?"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["answer"] == "Delta has the best on-time rate."
        assert "get_carrier_performance" in data["tools_used"]


def test_chat_agent_error_returns_500(client):
    """When the agent throws an exception, we should get a 500 with a clean error message."""
    with patch("api.routers.chat.settings") as mock_settings, \
         patch("api.routers.chat.run_agent", new_callable=AsyncMock, side_effect=RuntimeError("LLM timeout")):
        mock_settings.anthropic_api_key = "test-key"
        resp = client.post("/api/v1/chat", json={"question": "test"})
        assert resp.status_code == 500
        assert "Agent error" in resp.json()["detail"]
