package safe_socket

import (
	"fmt"
	"io"
)

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	bytesSent := 0

	for bytesSent < len(bytes) {
		n, err := socket.Write(bytes[bytesSent:])
		bytesSent += n

		if err != nil {
			return err
		}
	}

	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("receive size must be non-negative: %d", size)
	}

	buffer := make([]byte, size)
	bytesRead := 0

	for bytesRead < size {
		n, err := socket.Read(buffer[bytesRead:])
		bytesRead += n

		if bytesRead == size {
			return buffer, nil
		}
		if err != nil {
			return nil, err
		}
	}

	return buffer, nil
}
