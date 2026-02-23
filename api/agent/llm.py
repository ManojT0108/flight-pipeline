"""
LLM factory — creates a configured Anthropic chat model.

Using Sonnet because it's the sweet spot for tool-calling tasks:
smart enough to pick the right tools, cost-effective at ~$0.01-0.03 per query.
Temperature 0 for deterministic, factual answers (not creative writing).
"""
from langchain_anthropic import ChatAnthropic
from api.config import settings


def get_llm() -> ChatAnthropic:
    """Build a ChatAnthropic instance with project-wide settings."""
    return ChatAnthropic(
        model="claude-sonnet-4-20250514",
        api_key=settings.anthropic_api_key,
        max_tokens=1024,
        temperature=0,
    )
