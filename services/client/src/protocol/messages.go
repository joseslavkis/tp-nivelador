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

func (encoder *BetBatchEncoder) Append(
	agencyID uint32,
	firstName []byte,
	lastName []byte,
	document uint64,
	birthdate []byte,
	number uint32,
) error {
	if len(encoder.payload) < uint32Size {
		encoder.Reset(encoder.payload)
	}
	if err := validateTextBytes(firstName, "first name"); err != nil {
		return err
	}
	if err := validateTextBytes(lastName, "last name"); err != nil {
		return err
	}
	if err := validateTextBytes(birthdate, "birthdate"); err != nil {
		return err
	}
	if encoder.count == ^uint32(0) {
		return fmt.Errorf("bet batch contains too many bets")
	}

	encodedBetSize := uint32Size + encodedBytesSize(firstName) + encodedBytesSize(lastName) +
		uint64Size + encodedBytesSize(birthdate) + uint32Size
	if encodedBetSize > maxBatchPayloadSize-len(encoder.payload)-uint32Size {
		return fmt.Errorf("bet batch payload exceeds %d bytes", maxBatchPayloadSize)
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
	return nil
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

func EncodeBet(bet BetPayload) ([]byte, error) {
	if err := validateBet(bet); err != nil {
		return nil, err
	}

	payload := make([]byte, betPayloadSize(bet))
	writeBet(payload, bet)
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

func EncodeBetBatch(bets []BetPayload) ([]byte, error) {
	if len(bets) == 0 {
		return nil, fmt.Errorf("bet batch cannot be empty")
	}
	if uint64(len(bets)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("bet batch contains too many bets: %d", len(bets))
	}

	payloadSize := uint32Size
	for index, bet := range bets {
		if err := validateBet(bet); err != nil {
			return nil, fmt.Errorf("encode bet %d: %w", index, err)
		}
		encodedBetSize := betPayloadSize(bet)
		if encodedBetSize > maxBatchPayloadSize-payloadSize-uint32Size {
			return nil, fmt.Errorf("bet batch payload exceeds %d bytes", maxBatchPayloadSize)
		}
		payloadSize += uint32Size + encodedBetSize
	}

	payload := make([]byte, payloadSize)
	binary.BigEndian.PutUint32(payload, uint32(len(bets)))
	offset := uint32Size
	for _, bet := range bets {
		encodedBetSize := betPayloadSize(bet)
		binary.BigEndian.PutUint32(payload[offset:], uint32(encodedBetSize))
		offset += uint32Size
		writeBet(payload[offset:offset+encodedBetSize], bet)
		offset += encodedBetSize
	}

	return payload, nil
}

func DecodeBetBatch(payload []byte) ([]BetPayload, error) {
	if len(payload) > maxBatchPayloadSize {
		return nil, fmt.Errorf("bet batch payload exceeds %d bytes", maxBatchPayloadSize)
	}
	if len(payload) < uint32Size {
		return nil, fmt.Errorf("bet batch payload is missing bet count")
	}

	betCount := binary.BigEndian.Uint32(payload)
	if betCount == 0 {
		return nil, fmt.Errorf("bet batch cannot be empty")
	}

	bets := make([]BetPayload, 0)
	offset := uint32Size
	for index := uint32(0); index < betCount; index++ {
		if len(payload)-offset < uint32Size {
			return nil, fmt.Errorf("bet batch payload is missing length for bet %d", index)
		}
		betLength := int(binary.BigEndian.Uint32(payload[offset:]))
		offset += uint32Size
		if betLength > len(payload)-offset {
			return nil, fmt.Errorf("bet batch payload is missing data for bet %d", index)
		}

		bet, err := DecodeBet(payload[offset : offset+betLength])
		if err != nil {
			return nil, fmt.Errorf("decode bet %d: %w", index, err)
		}
		bets = append(bets, bet)
		offset += betLength
	}

	if offset != len(payload) {
		return nil, fmt.Errorf("bet batch payload has %d trailing bytes", len(payload)-offset)
	}

	return bets, nil
}

func validateBet(bet BetPayload) error {
	if err := validateText(bet.FirstName, "first name"); err != nil {
		return err
	}
	if err := validateText(bet.LastName, "last name"); err != nil {
		return err
	}
	return validateText(bet.Birthdate, "birthdate")
}

func validateText(value string, field string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", field)
	}
	if len(value) > maxTextLength {
		return fmt.Errorf("%s exceeds %d bytes", field, maxTextLength)
	}
	return nil
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

func betPayloadSize(bet BetPayload) int {
	return uint32Size + encodedTextSize(bet.FirstName) + encodedTextSize(bet.LastName) +
		uint64Size + encodedTextSize(bet.Birthdate) + uint32Size
}

func encodedTextSize(value string) int {
	return uint16Size + len(value)
}

func encodedBytesSize(value []byte) int {
	return uint16Size + len(value)
}

func writeBet(payload []byte, bet BetPayload) {
	offset := 0
	binary.BigEndian.PutUint32(payload[offset:], bet.AgencyID)
	offset += uint32Size

	offset = writeText(payload, offset, bet.FirstName)
	offset = writeText(payload, offset, bet.LastName)

	binary.BigEndian.PutUint64(payload[offset:], bet.Document)
	offset += uint64Size

	offset = writeText(payload, offset, bet.Birthdate)
	binary.BigEndian.PutUint32(payload[offset:], bet.Number)
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

func writeText(payload []byte, offset int, value string) int {
	binary.BigEndian.PutUint16(payload[offset:], uint16(len(value)))
	offset += uint16Size
	copy(payload[offset:], value)
	return offset + len(value)
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
