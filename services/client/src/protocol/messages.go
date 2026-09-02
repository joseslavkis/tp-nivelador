package protocol

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

const (
	uint16Size              = 2
	uint32Size              = 4
	uint64Size              = 8
	maxTextLength           = 65535
	maxBatchPayloadSize     = 16 * 1024 * 1024
	betBatchInitialCapacity = 1024
)

type BetPayload struct {
	AgencyID  uint32
	FirstName string
	LastName  string
	Document  uint64
	Birthdate string
	Number    uint32
}

type BetBatchEncoder struct {
	payload []byte
	count   uint32
}

func (encoder *BetBatchEncoder) Reset(payload []byte) {
	if cap(payload) < uint32Size {
		payload = make([]byte, uint32Size, betBatchInitialCapacity)
	} else {
		payload = payload[:uint32Size]
	}
	encoder.payload = payload
	encoder.count = 0
}

func (encoder *BetBatchEncoder) TryAppend(
	agencyID uint32,
	firstName []byte,
	lastName []byte,
	document uint64,
	birthdate []byte,
	number uint32,
) (bool, error) {
	if len(encoder.payload) < uint32Size {
		encoder.Reset(encoder.payload)
	}
	if err := validateTextBytes(firstName, "first name"); err != nil {
		return false, err
	}
	if err := validateTextBytes(lastName, "last name"); err != nil {
		return false, err
	}
	if err := validateTextBytes(birthdate, "birthdate"); err != nil {
		return false, err
	}
	if encoder.count == ^uint32(0) {
		return false, fmt.Errorf("bet batch contains too many bets")
	}

	encodedBetSize := uint32Size + encodedBytesSize(firstName) + encodedBytesSize(lastName) +
		uint64Size + encodedBytesSize(birthdate) + uint32Size
	if encodedBetSize > maxBatchPayloadSize-len(encoder.payload)-uint32Size {
		return false, nil
	}

	offset := len(encoder.payload)
	encoder.payload = append(encoder.payload, make([]byte, uint32Size+encodedBetSize)...)
	binary.BigEndian.PutUint32(encoder.payload[offset:], uint32(encodedBetSize))
	writeBetBytes(
		encoder.payload[offset+uint32Size:],
		agencyID,
		firstName,
		lastName,
		document,
		birthdate,
		number,
	)
	encoder.count++
	return true, nil
}

func (encoder *BetBatchEncoder) Count() int {
	return int(encoder.count)
}

func (encoder *BetBatchEncoder) Payload() ([]byte, error) {
	if encoder.count == 0 {
		return nil, fmt.Errorf("bet batch cannot be empty")
	}
	binary.BigEndian.PutUint32(encoder.payload, encoder.count)
	return encoder.payload, nil
}

func EncodeAgencyID(agencyID uint32) []byte {
	payload := make([]byte, uint32Size)
	binary.BigEndian.PutUint32(payload, agencyID)
	return payload
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

func validateTextBytes(value []byte, field string) error {
	if !utf8.Valid(value) {
		return fmt.Errorf("%s is not valid UTF-8", field)
	}
	if len(value) > maxTextLength {
		return fmt.Errorf("%s exceeds %d bytes", field, maxTextLength)
	}
	return nil
}

func encodedBytesSize(value []byte) int {
	return uint16Size + len(value)
}

func writeBetBytes(
	payload []byte,
	agencyID uint32,
	firstName []byte,
	lastName []byte,
	document uint64,
	birthdate []byte,
	number uint32,
) {
	offset := 0
	binary.BigEndian.PutUint32(payload[offset:], agencyID)
	offset += uint32Size

	offset = writeBytes(payload, offset, firstName)
	offset = writeBytes(payload, offset, lastName)

	binary.BigEndian.PutUint64(payload[offset:], document)
	offset += uint64Size

	offset = writeBytes(payload, offset, birthdate)
	binary.BigEndian.PutUint32(payload[offset:], number)
}

func writeBytes(payload []byte, offset int, value []byte) int {
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
