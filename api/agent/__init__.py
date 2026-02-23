"""
Flight data AI agent — natural language interface to the pipeline.

Usage:
    from api.agent import run_agent
    result = await run_agent("Which airline has the worst delays?")
    # result = {"answer": "...", "tools_used": ["get_carrier_performance"]}
"""
from api.agent.graph import run_agent

__all__ = ["run_agent"]
