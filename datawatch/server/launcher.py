"""Programmatic server launcher for Datawatch."""

import logging
import threading
import time
import webbrowser

import uvicorn

logger = logging.getLogger(__name__)


def launch(host: str, port: int, open_browser: bool = True) -> None:
    """Start the Datawatch server and optionally open the dashboard URL."""
    browser_url = "http://localhost:{0}".format(port)

    if open_browser:
        def _open_browser_after_delay() -> None:
            time.sleep(1.5)
            webbrowser.open(browser_url)

        threading.Thread(target=_open_browser_after_delay, daemon=True).start()

    config = uvicorn.Config(
        app="datawatch.server.app:app",
        host=host,
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(config=config)

    try:
        server.run()
    except KeyboardInterrupt:
        logger.info("Shutdown requested, stopping Datawatch server.")
