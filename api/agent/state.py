"""
Shared state that flows between nodes in the agent graph.

The key insight here is the `add_messages` annotation — it tells LangGraph
to APPEND new messages instead of replacing the list. Without it, each node
would overwrite everything the previous node produced.
"""
from typing import Annotated
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage


class AgentState(TypedDict):
    # The full conversation history: system prompt, user question,
    # LLM responses, tool results — all accumulate here as the loop runs.
    messages: Annotated[list[BaseMessage], add_messages]
