#!/usr/bin/env python
"""Run a Reflex example with the API server.

This script dynamically imports an example module (registering its triggers),
then starts the uvicorn server.

Usage:
    python scripts/run_example.py basic
    python scripts/run_example.py fraud_detection
    python scripts/run_example.py --list

Requires the server dependencies (PostgreSQL) to be running:
    docker compose up -d db
"""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path

# Add project root to path so examples can be imported
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Available examples
EXAMPLES = [
    "basic",
    "fraud_detection",
    "incident_response",
    "log_anomaly",
    "support_bot",
    "content_moderation",
]


EXAMPLE_DESCRIPTIONS = {
    "basic": "Error monitoring with alert triggers",
    "fraud_detection": "E-commerce order fraud detection with LLM",
    "incident_response": "Automated incident response system",
    "log_anomaly": "Log anomaly detection agent",
    "support_bot": "Customer support chatbot",
    "content_moderation": "Content moderation with AI classification",
}


def list_examples() -> None:
    """Print available examples."""
    print("Available examples:")
    print()
    for name in EXAMPLES:
        desc = EXAMPLE_DESCRIPTIONS.get(name, "No description")
        print(f"  {name:<20} {desc}")
    print()
    print("Usage:")
    print("  make example EXAMPLE=basic")
    print("  python scripts/run_example.py basic")
    print()
    print("Note: Examples require ANTHROPIC_API_KEY and a running database:")
    print("      export ANTHROPIC_API_KEY=sk-ant-...")
    print("      docker compose up db -d")


def load_example(name: str) -> None:
    """Import an example module to register its triggers."""
    if name not in EXAMPLES:
        print(f"Unknown example: {name}")
        print(f"Available: {', '.join(EXAMPLES)}")
        sys.exit(1)

    module_name = f"examples.{name}"
    print(f"Loading example: {module_name}")

    try:
        module = importlib.import_module(module_name)
        doc = module.__doc__ or name
        # Get first line of docstring
        desc = doc.strip().split("\n")[0]
        print(f"  {desc}")
    except ImportError as e:
        print(f"Failed to import {module_name}: {e}")
        sys.exit(1)
    except Exception as e:
        error_msg = str(e)
        if "api_key" in error_msg.lower() or "OPENAI" in error_msg or "ANTHROPIC" in error_msg:
            print(f"\nError: Missing API key")
            print("Set the required environment variable:")
            print("  export OPENAI_API_KEY=sk-...")
            print("  export ANTHROPIC_API_KEY=sk-ant-...")
        else:
            print(f"Failed to load {module_name}: {e}")
        sys.exit(1)

    # Show registered triggers from this example
    from reflex import get_registry

    registry = get_registry()
    print(f"\nRegistered triggers: {len(registry.triggers)}")
    for trigger in registry.triggers:
        print(f"  • {trigger.name} (priority: {trigger.priority})")
    print()


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run a Reflex example with the API server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_example.py basic
  python scripts/run_example.py fraud_detection --port 8001
  python scripts/run_example.py --list
        """,
    )
    parser.add_argument(
        "example",
        nargs="?",
        help="Example name to run",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available examples",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind to (default: 8000)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development",
    )

    args = parser.parse_args()

    if args.list:
        list_examples()
        return

    if not args.example:
        parser.print_help()
        print("\nError: example name required (or use --list)")
        sys.exit(1)

    # Load the example (registers triggers)
    load_example(args.example)

    # Start the server
    import uvicorn

    print(f"Starting server on {args.host}:{args.port}")
    print("Press Ctrl+C to stop\n")

    uvicorn.run(
        "reflex.api.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info",
    )


if __name__ == "__main__":
    main()
