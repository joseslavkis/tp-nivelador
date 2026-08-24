package protocol

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

const (
	uint16Size    = 2
	uint32Size    = 4
	uint64Size    = 8
	maxTextLength = 65535
)

type BetPayload struct {
	AgencyID  uint32
	FirstName string
	LastName  string
	Document  uint64
	Birthdate string
	Number    uint32
}

func EncodeBet(bet BetPayload) ([]byte, error) {
	firstName, err := encodeText(bet.FirstName, "first name")
	if err != nil {
		return nil, err
	}
	lastName, err := encodeText(bet.LastName, "last name")
	if err != nil {
		return nil, err
	}
	birthdate, err := encodeText(bet.Birthdate, "birthdate")
	if err != nil {
		return nil, err
	}

	payloadSize := uint32Size + encodedTextSize(firstName) + encodedTextSize(lastName) +
		uint64Size + encodedTextSize(birthdate) + uint32Size
	payload := make([]byte, payloadSize)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], bet.AgencyID)
	offset += uint32Size

	offset = writeText(payload, offset, firstName)
	offset = writeText(payload, offset, lastName)

	binary.BigEndian.PutUint64(payload[offset:], bet.Document)
	offset += uint64Size

	offset = writeText(payload, offset, birthdate)

	binary.BigEndian.PutUint32(payload[offset:], bet.Number)

	return payload, nil
}

func DecodeBet(payload []byte) (BetPayload, error) {
	decoder := payloadDecoder{payload: payload}

	agencyID, err := decoder.readUint32("agency id")
	if err != nil {
		return BetPayload{}, err
	}
	firstName, err := decoder.readText("first name")
	if err != nil {
		return BetPayload{}, err
	}
	lastName, err := decoder.readText("last name")
	if err != nil {
		return BetPayload{}, err
	}
	document, err := decoder.readUint64("document")
	if err != nil {
		return BetPayload{}, err
	}
	birthdate, err := decoder.readText("birthdate")
	if err != nil {
		return BetPayload{}, err
	}
	number, err := decoder.readUint32("number")
	if err != nil {
		return BetPayload{}, err
	}
	if decoder.remaining() != 0 {
		return BetPayload{}, fmt.Errorf("bet payload has %d trailing bytes", decoder.remaining())
	}

	return BetPayload{
		AgencyID:  agencyID,
		FirstName: firstName,
		LastName:  lastName,
		Document:  document,
		Birthdate: birthdate,
		Number:    number,
	}, nil
}

func encodeText(value string, field string) ([]byte, error) {
	if !utf8.ValidString(value) {
		return nil, fmt.Errorf("%s is not valid UTF-8", field)
	}
	encoded := []byte(value)
	if len(encoded) > maxTextLength {
		return nil, fmt.Errorf("%s exceeds %d bytes", field, maxTextLength)
	}
	return encoded, nil
}

func encodedTextSize(value []byte) int {
	return uint16Size + len(value)
}

func writeText(payload []byte, offset int, value []byte) int {
	binary.BigEndian.PutUint16(payload[offset:], uint16(len(value)))
	offset += uint16Size
	copy(payload[offset:], value)
	return offset + len(value)
}

type payloadDecoder struct {
	payload []byte
	offset  int
}

func (decoder *payloadDecoder) readUint32(field string) (uint32, error) {
	value, err := decoder.readBytes(uint32Size, field)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(value), nil
}

func (decoder *payloadDecoder) readUint64(field string) (uint64, error) {
	value, err := decoder.readBytes(uint64Size, field)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(value), nil
}

func (decoder *payloadDecoder) readText(field string) (string, error) {
	lengthBytes, err := decoder.readBytes(uint16Size, field+" length")
	if err != nil {
		return "", err
	}
	length := int(binary.BigEndian.Uint16(lengthBytes))
	value, err := decoder.readBytes(length, field)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(value) {
		return "", fmt.Errorf("%s is not valid UTF-8", field)
	}
	return string(value), nil
}

func (decoder *payloadDecoder) readBytes(size int, field string) ([]byte, error) {
	if decoder.remaining() < size {
		return nil, fmt.Errorf("bet payload is missing %s", field)
	}
	value := decoder.payload[decoder.offset : decoder.offset+size]
	decoder.offset += size
	return value, nil
}

func (decoder *payloadDecoder) remaining() int {
	return len(decoder.payload) - decoder.offset
}
