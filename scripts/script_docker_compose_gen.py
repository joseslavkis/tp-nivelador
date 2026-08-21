import sys


OUTPUT_FILE = "docker-compose.yaml"
SERVER_CONTEXT = "./services/server"
CLIENT_CONTEXT = "./services/client"
DOCKERFILE = "Dockerfile"
SERVER_HOST = "server"
SERVER_PORT = 5678
PYTHON_UNBUFFERED = 1


def get_server_lines():
    return [
        "services:",
        "  server:",
        "    build:",
        f"      context: {SERVER_CONTEXT}",
        f"      dockerfile: {DOCKERFILE}",
        f"    container_name: {SERVER_HOST}",
        "    environment:",
        f"      - PYTHONUNBUFFERED={PYTHON_UNBUFFERED}",
        f"      - SERVER_HOST={SERVER_HOST}",
        f"      - SERVER_PORT={SERVER_PORT}",
    ]


def get_client_lines(client_id):
    return [
        f"  client_{client_id}:",
        "    build:",
        f"      context: {CLIENT_CONTEXT}",
        f"      dockerfile: {DOCKERFILE}",
        f"    container_name: client_{client_id}",
        "    depends_on:",
        f"      - {SERVER_HOST}",
        "    environment:",
        f"      - AGENCY_ID={client_id}",
        f"      - SERVER_HOST={SERVER_HOST}",
        f"      - SERVER_PORT={SERVER_PORT}",
    ]


def build_file(client_count):
    lines = get_server_lines()

    for client_id in range(client_count):
        lines += [""] + get_client_lines(client_id)

    return "\n".join(lines) + "\n"


def main():
    if len(sys.argv) < 2:
        print("use: python3 scripts/script_docker_compose_gen.py <client_count> [output]")
        return 1

    client_count = int(sys.argv[1])
    output_file = OUTPUT_FILE

    if len(sys.argv) >= 3:
        output_file = sys.argv[2]

    if client_count < 1:
        print("client_count must be greater than 0")
        return 1

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(build_file(client_count))

    return 0


if __name__ == "__main__":
    sys.exit(main())
