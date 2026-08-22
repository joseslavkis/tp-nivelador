package safe_socket

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	messageTypeSize   = 1
	payloadLengthSize = 4
	messageHeaderSize = messageTypeSize + payloadLengthSize
)

type MessageType byte

const (
	MessageTypeBet    MessageType = 1
	MessageTypeEnd    MessageType = 2
	MessageTypeWinner MessageType = 3
	MessageTypeError  MessageType = 4
)

type Message struct {
	Type    MessageType
	Payload []byte
}

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

func SendMessage(socket io.Writer, message Message) error {
	header := make([]byte, messageHeaderSize)
	header[0] = byte(message.Type)
	binary.BigEndian.PutUint32(header[1:], uint32(len(message.Payload)))

	if err := SendAll(socket, header); err != nil {
		return err
	}

	return SendAll(socket, message.Payload)
}

func RecvMessage(socket io.Reader) (Message, error) {
	header, err := RecvAll(socket, messageHeaderSize)
	if err != nil {
		return Message{}, err
	}

	payloadSize := binary.BigEndian.Uint32(header[1:])
	payload, err := RecvAll(socket, int(payloadSize))
	if err != nil {
		return Message{}, err
	}

	return Message{
		Type:    MessageType(header[0]),
		Payload: payload,
	}, nil
}
