import socket
from weakref import WeakKeyDictionary

MESSAGE_TYPE_SIZE = 1
PAYLOAD_LENGTH_SIZE = 4
MESSAGE_HEADER_SIZE = MESSAGE_TYPE_SIZE + PAYLOAD_LENGTH_SIZE

MAX_MESSAGE_TYPE = 255
MAX_PAYLOAD_SIZE = 16 * 1024 * 1024
MAX_EMPTY_OPERATIONS = 100

_RECEIVE_BUFFERS: WeakKeyDictionary[socket.socket, bytearray] = WeakKeyDictionary()


def recv_all(sock: socket.socket, size: int) -> bytes:
    if size < 0:
        raise ValueError(f"receive size must be non-negative: {size}")
    if size == 0:
        return b""

    received = _RECEIVE_BUFFERS.pop(sock, bytearray())

    while len(received) < size:
        chunk = sock.recv(size)
        if not chunk:
            raise ConnectionError("connection closed while receiving data")
        received.extend(chunk)

    result = bytes(received[:size])
    del received[:size]
    if received:
        _RECEIVE_BUFFERS[sock] = received

    return result


def send_all(sock: socket.socket, data: bytes) -> None:
    bytes_sent = 0
    empty_writes = 0

    while bytes_sent < len(data):
        sent = sock.send(data[bytes_sent:])
        if sent == 0:
            empty_writes += 1
            if empty_writes >= MAX_EMPTY_OPERATIONS:
                raise ConnectionError("socket made no progress while sending data")
            continue

        empty_writes = 0
        bytes_sent += sent


def send_message(sock: socket.socket, message_type: int, payload: bytes) -> None:
    if not 0 <= message_type <= MAX_MESSAGE_TYPE:
        raise ValueError(f"invalid message type: {message_type}")
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError(f"payload exceeds {MAX_PAYLOAD_SIZE} bytes")

    header = bytes([message_type]) + len(payload).to_bytes(
        PAYLOAD_LENGTH_SIZE, byteorder="big"
    )
    send_all(sock, header)
    send_all(sock, payload)


def recv_message(sock: socket.socket) -> tuple[int, bytes]:
    header = recv_all(sock, MESSAGE_HEADER_SIZE)
    message_type = header[0]
    payload_size = int.from_bytes(header[MESSAGE_TYPE_SIZE:], byteorder="big")
    if payload_size > MAX_PAYLOAD_SIZE:
        raise ValueError(f"payload exceeds {MAX_PAYLOAD_SIZE} bytes")
    payload = recv_all(sock, payload_size)
    return message_type, payload
