"""reflexr: rules over event streams that run LLM workflows.

Applications publish events into tenant-scoped streams. Rules watch the streams, and when a
rule's condition holds, it runs a workflow: a pydantic-ai agent, a pydantic-graph graph, or a
plain async function. See ``docs/architecture.md`` for the design.
"""

from importlib.metadata import version

__version__ = version("reflexr")

__all__ = ["__version__"]
