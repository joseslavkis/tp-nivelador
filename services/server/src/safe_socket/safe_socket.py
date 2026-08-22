import socket

MESSAGE_HEADER_SIZE = 5

MESSAGE_TYPE_BET = 1
MESSAGE_TYPE_END = 2
MESSAGE_TYPE_WINNER = 3
MESSAGE_TYPE_ERROR = 4
MAX_MESSAGE_TYPE_SIZE = 255

def recv_all(socket: socket.socket, size: int) -> bytes:
    received_chunks = []
    bytes_received = 0

    while bytes_received < size:
        chunk = socket.recv(size - bytes_received)
        received_chunks.append(chunk)
        bytes_received += len(chunk)

    return b"".join(received_chunks)

def send_all(socket: socket.socket, data: bytes) -> None:
    bytes_sent = 0

    while bytes_sent < len(data):
        sent = socket.send(data[bytes_sent:])
        bytes_sent += sent

def send_message(socket: socket.socket, message_type: int, payload: bytes) -> None:
    if not 0 <= message_type <= MAX_MESSAGE_TYPE_SIZE:
        raise ValueError(f"invalid message type: {message_type}")

    header = bytes([message_type]) + len(payload).to_bytes(4, byteorder="big")
    send_all(socket, header)
    send_all(socket, payload)

def recv_message(socket: socket.socket) -> tuple[int, bytes]:
    header = recv_all(socket, MESSAGE_HEADER_SIZE)
    message_type = header[0]
    payload_size = int.from_bytes(header[1:], byteorder="big")
    payload = recv_all(socket, payload_size)
    return message_type, payload

