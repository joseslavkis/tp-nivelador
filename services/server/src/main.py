import os
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
    logger.init()
    try:
        server_instance = server.Server(
            server_host=os.environ["SERVER_HOST"],
            server_port=int(os.environ["SERVER_PORT"]),
            storage_directory=os.environ.get("STORAGE_DIRECTORY", "/tmp/lottery"),
            agency_quorum_min=_load_agency_quorum_min(),
        )
        server_instance.run()
    except Exception as error:
        logger.error("server-run", logger.LogResult.fail, "err", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
