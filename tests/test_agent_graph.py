"""
Tests for the agent graph structure and state.

These verify the foundational pieces without needing a real LLM:
- AgentState schema is correct
- All 9 tools are registered (catches accidental removals)
- The graph compiles without errors (validates node/edge wiring)
"""
from unittest.mock import patch, MagicMock


def test_agent_state_has_messages():
    """AgentState should have a messages key for the conversation history."""
    from api.agent.state import AgentState
    state = AgentState(messages=[])
    assert "messages" in state


def test_all_tools_are_registered():
    """ALL_TOOLS should contain exactly 9 tools — one for each data domain."""
    from api.agent.tools import ALL_TOOLS
    assert len(ALL_TOOLS) == 9
    names = {t.name for t in ALL_TOOLS}
    expected = {
        "get_carrier_performance",
        "get_carrier_details",
        "get_delay_patterns",
        "get_route_info",
        "get_weather_impact",
        "get_weather_origin_vs_dest",
        "get_cascade_events",
        "get_airport_info",
        "get_system_health",
    }
    assert names == expected


def test_graph_builds_and_compiles():
    """The two-node ReAct graph should compile without errors."""
    mock_llm = MagicMock()
    mock_llm.bind_tools.return_value = mock_llm

    with patch("api.agent.graph.get_llm", return_value=mock_llm), \
         patch("api.agent.graph._compiled", None):
        from api.agent.graph import _build_graph
        graph = _build_graph()
        compiled = graph.compile()
        # Verify both nodes exist in the compiled graph
        assert "agent" in compiled.nodes
        assert "tools" in compiled.nodes
