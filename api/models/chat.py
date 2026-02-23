"""
Pydantic models for the chat endpoint.

ChatRequest validates the incoming question (enforcing length limits to prevent
abuse and empty submissions). ChatResponse structures the agent's output so
the client knows both what the agent said and which tools it used to get there.
"""
from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="Natural language question about flight data",
    )


class ChatResponse(BaseModel):
    answer: str
    tools_used: list[str] = []
