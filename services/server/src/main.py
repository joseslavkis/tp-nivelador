import os
import sys

import logger
import server

def main() -> int:
    logger.init()
    try:
        server_instance = server.Server(
            server_host=os.environ["SERVER_HOST"],
            server_port=int(os.environ["SERVER_PORT"]),
            storage_directory=os.environ.get("STORAGE_DIRECTORY", "/tmp/lottery"),
        )
        server_instance.run()
    except Exception as error:
        logger.error("server-run", logger.LogResult.fail, "err", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
