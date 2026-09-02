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

const MessageHeaderSize = messageHeaderSize

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
	if err := recvAllInto(socket, buffer); err != nil {
		return nil, err
	}
	return buffer, nil
}

func recvAllInto(socket io.Reader, buffer []byte) error {
	bytesRead := 0
	emptyReads := 0

	for bytesRead < len(buffer) {
		n, err := socket.Read(buffer[bytesRead:])
		bytesRead += n

		if bytesRead == len(buffer) {
			return nil
		}
		if err != nil {
			return err
		}
		if n == 0 {
			emptyReads++
			if emptyReads >= maxEmptyOperations {
				return io.ErrNoProgress
			}
			continue
		}
		emptyReads = 0
	}

	return nil
}

func SendMessageWithHeader(socket io.Writer, message Message, header []byte) error {
	if !message.Type.isValid() {
		return fmt.Errorf("invalid message type %d", message.Type)
	}
	if len(message.Payload) > maxPayloadSize {
		return fmt.Errorf("payload exceeds %d bytes", maxPayloadSize)
	}
	if len(header) < messageHeaderSize {
		return fmt.Errorf("message header buffer must contain at least %d bytes", messageHeaderSize)
	}

	header[0] = byte(message.Type)
	binary.BigEndian.PutUint32(header[1:], uint32(len(message.Payload)))

	if err := SendAll(socket, header[:messageHeaderSize]); err != nil {
		return err
	}

	return SendAll(socket, message.Payload)
}

func RecvMessageInto(socket io.Reader, header []byte, payload []byte) (Message, error) {
	if len(header) < messageHeaderSize {
		return Message{}, fmt.Errorf("message header buffer must contain at least %d bytes", messageHeaderSize)
	}
	if err := recvAllInto(socket, header[:messageHeaderSize]); err != nil {
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
	if cap(payload) < payloadSize {
		payload = make([]byte, payloadSize)
	} else {
		payload = payload[:payloadSize]
	}
	if err := recvAllInto(socket, payload); err != nil {
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
