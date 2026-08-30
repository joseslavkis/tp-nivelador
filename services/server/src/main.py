import os
import signal
import sys

import logger
import server


DEFAULT_AGENCY_QUORUM_MIN = 1


def _load_agency_quorum_min() -> int:
    value = os.environ.get(
        "AGENCY_QUORUM_MIN", str(DEFAULT_AGENCY_QUORUM_MIN)
    )
    try:
        agency_quorum_min = int(value)
    except ValueError as error:
        raise ValueError(
            "AGENCY_QUORUM_MIN must be a positive integer"
        ) from error
    if agency_quorum_min <= 0:
        raise ValueError("AGENCY_QUORUM_MIN must be a positive integer")
    return agency_quorum_min


def main() -> int:
    server_instance: server.Server | None = None
    shutdown_requested = False

    def handle_sigterm(_signum, _frame) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True
        if server_instance is not None:
            server_instance.request_shutdown()

    signal.signal(signal.SIGTERM, handle_sigterm)
    logger.init()
    try:
        server_instance = server.Server(
            server_host=os.environ["SERVER_HOST"],
            server_port=int(os.environ["SERVER_PORT"]),
            storage_directory=os.environ.get("STORAGE_DIRECTORY", "/tmp/lottery"),
            agency_quorum_min=_load_agency_quorum_min(),
        )
        if shutdown_requested:
            server_instance.request_shutdown()
        server_instance.run()
    except Exception as error:
        if shutdown_requested:
            return 0
        logger.error("server-run", logger.LogResult.fail, "err", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
