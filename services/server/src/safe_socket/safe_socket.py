import socket


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
