package safe_socket

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	messageTypeSize    = 1
	payloadLengthSize  = 4
	messageHeaderSize  = messageTypeSize + payloadLengthSize
	maxPayloadSize     = 16 * 1024 * 1024
	maxEmptyOperations = 100
)

type MessageType byte

const (
	MessageTypeBet       MessageType = 1
	MessageTypeEnd       MessageType = 2
	MessageTypeWinner    MessageType = 3
	MessageTypeError     MessageType = 4
	MessageTypeBetsBatch MessageType = 5
	MessageTypeBatchAck  MessageType = 6
)

type Message struct {
	Type    MessageType
	Payload []byte
}

func SendAll(socket io.Writer, bytes []byte) error {
	bytesSent := 0
	emptyWrites := 0

	for bytesSent < len(bytes) {
		n, err := socket.Write(bytes[bytesSent:])
		bytesSent += n

		if err != nil {
			return err
		}
		if n == 0 {
			emptyWrites++
			if emptyWrites >= maxEmptyOperations {
				return io.ErrNoProgress
			}
			continue
		}
		emptyWrites = 0
	}

	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("receive size must be non-negative: %d", size)
	}

	buffer := make([]byte, size)
	bytesRead := 0
	emptyReads := 0

	for bytesRead < size {
		n, err := socket.Read(buffer[bytesRead:])
		bytesRead += n

		if bytesRead == size {
			return buffer, nil
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			emptyReads++
			if emptyReads >= maxEmptyOperations {
				return nil, io.ErrNoProgress
			}
			continue
		}
		emptyReads = 0
	}

	return buffer, nil
}

func SendMessage(socket io.Writer, message Message) error {
	if !message.Type.isValid() {
		return fmt.Errorf("invalid message type %d", message.Type)
	}
	if len(message.Payload) > maxPayloadSize {
		return fmt.Errorf("payload exceeds %d bytes", maxPayloadSize)
	}

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

	messageType := MessageType(header[0])
	if !messageType.isValid() {
		return Message{}, fmt.Errorf("invalid message type %d", messageType)
	}
	payloadSize := int(binary.BigEndian.Uint32(header[1:]))
	if payloadSize > maxPayloadSize {
		return Message{}, fmt.Errorf("payload exceeds %d bytes", maxPayloadSize)
	}
	payload, err := RecvAll(socket, payloadSize)
	if err != nil {
		return Message{}, err
	}

	return Message{
		Type:    messageType,
		Payload: payload,
	}, nil
}

func (messageType MessageType) isValid() bool {
	return messageType >= MessageTypeBet && messageType <= MessageTypeBatchAck
}
