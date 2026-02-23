"""
LangGraph ReAct agent — the core reasoning loop.

Architecture:
    [agent] → has tool_calls? → Yes → [tools] → back to [agent]
                               → No  → [END]  → return answer

The agent node sends the conversation to the LLM. If the LLM decides it needs
data, it returns tool_calls. The tools node executes them and feeds results back.
This loops until the LLM has enough information to write a final answer.

The graph is compiled once at startup and reused across all requests
to avoid repeated validation overhead on every incoming question.
"""
import logging

from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langchain_core.messages import HumanMessage, SystemMessage

from api.agent.state import AgentState
from api.agent.tools import ALL_TOOLS
from api.agent.llm import get_llm
from api.agent.prompts import SYSTEM_PROMPT

logger = logging.getLogger("flight-agent")


def _build_graph() -> StateGraph:
    """Wire up the two-node ReAct graph: agent ↔ tools."""

    # Bind all 9 tools so the LLM knows what's available
    llm = get_llm().bind_tools(ALL_TOOLS)

    def agent_node(state: AgentState) -> dict:
        """Send the full conversation history to the LLM and get a response.
        The response either contains tool_calls (needs more data) or
        plain content (ready to answer)."""
        response = llm.invoke(state["messages"])
        return {"messages": [response]}

    def should_continue(state: AgentState) -> str:
        """Route based on whether the LLM wants to call tools or is done."""
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "tools"
        return END

    # ToolNode handles dispatching — it reads tool_calls from the last message,
    # executes the matching Python functions, and adds ToolMessage results
    tool_node = ToolNode(ALL_TOOLS)

    # Assemble the graph
    graph = StateGraph(AgentState)
    graph.add_node("agent", agent_node)
    graph.add_node("tools", tool_node)
    graph.set_entry_point("agent")
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
    graph.add_edge("tools", "agent")  # after tools, always go back to agent

    return graph


# Lazy-compiled singleton — built on first request, reused after
_compiled = None


def _get_graph():
    """Get or build the compiled graph (singleton pattern)."""
    global _compiled
    if _compiled is None:
        _compiled = _build_graph().compile()
    return _compiled


async def run_agent(question: str) -> dict:
    """
    Main entry point: take a natural language question, run the ReAct loop,
    and return the answer with a list of tools that were called.

    Returns:
        {"answer": str, "tools_used": list[str]}
    """
    graph = _get_graph()

    # Seed the conversation with the system prompt and user's question
    initial_state = {
        "messages": [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=question),
        ]
    }

    # Run the loop — recursion_limit caps tool-calling iterations
    # to prevent runaway loops (e.g., LLM keeps asking for more data)
    result = await graph.ainvoke(initial_state, config={"recursion_limit": 10})

    # Walk through all messages to find the final answer and which tools were used
    answer = ""
    tools_used = []
    for msg in result["messages"]:
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            tools_used.extend(tc["name"] for tc in msg.tool_calls)
        # The final answer is the last AI message that has content but no tool calls
        if hasattr(msg, "content") and msg.content and not getattr(msg, "tool_calls", None):
            if msg.type == "ai":
                answer = msg.content

    # Deduplicate tools while preserving order
    return {"answer": answer, "tools_used": list(dict.fromkeys(tools_used))}
