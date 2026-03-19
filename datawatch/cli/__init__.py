"""Datawatch CLI package exports."""

__all__ = ["app"]


def __getattr__(name):
    """Lazily expose the top-level Typer app."""
    if name == "app":
        from datawatch.cli.main import app

        return app
    raise AttributeError("module 'datawatch.cli' has no attribute '{0}'".format(name))
