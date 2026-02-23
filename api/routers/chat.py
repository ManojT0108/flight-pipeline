"""
Chat router — the single endpoint that turns natural language into data insights.

This is the public-facing gateway to the LangGraph agent. It handles three things:
1. Graceful degradation when the API key isn't configured (503 instead of cryptic errors)
2. Running the agent's ReAct loop and returning structured results
3. Catching unexpected failures so they don't leak stack traces to the client
"""
import logging

from fastapi import APIRouter, HTTPException

from api.config import settings
from api.models.chat import ChatRequest, ChatResponse
from api.agent import run_agent

logger = logging.getLogger("flight-api.chat")

router = APIRouter()


@router.post("/", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Accept a natural language question, run the agent, return the answer.

    The flow: validate API key → run ReAct loop → return answer + tools used.
    We check for the API key at request time (not startup) so the rest of the API
    still works even if someone hasn't configured the chat feature yet.
    """

    # Fail fast if the key isn't set — better than a confusing LLM error downstream
    if not settings.anthropic_api_key:
        raise HTTPException(
            status_code=503,
            detail="Chat is unavailable — ANTHROPIC_API_KEY not configured",
        )

    try:
        result = await run_agent(request.question)
        return ChatResponse(answer=result["answer"], tools_used=result["tools_used"])
    except Exception as e:
        # Log the full traceback for debugging, but only send a clean message to the client
        logger.exception("Agent error: %s", e)
        raise HTTPException(status_code=500, detail=f"Agent error: {str(e)}")
