package client

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

const (
	csvFieldCount    = 5
	maxCSVRecordSize = 512 * 1024
)

var errUnterminatedQuotedField = errors.New("unterminated quoted field")

type csvBytesReader struct {
	reader       *bufio.Reader
	recordBuffer []byte
	quotedFields []byte
}

func newCSVBytesReader(reader io.Reader) *csvBytesReader {
	return &csvBytesReader{reader: bufio.NewReader(reader)}
}

func (reader *csvBytesReader) Read() ([csvFieldCount][]byte, error) {
	reader.recordBuffer = reader.recordBuffer[:0]
	for {
		fragment, readErr := reader.reader.ReadSlice('\n')
		reader.recordBuffer = append(reader.recordBuffer, fragment...)
		if len(reader.recordBuffer) > maxCSVRecordSize {
			return [csvFieldCount][]byte{}, fmt.Errorf(
				"record exceeds maximum size of %d bytes",
				maxCSVRecordSize,
			)
		}
		if errors.Is(readErr, bufio.ErrBufferFull) {
			continue
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return [csvFieldCount][]byte{}, readErr
		}
		if errors.Is(readErr, io.EOF) && len(reader.recordBuffer) == 0 {
			return [csvFieldCount][]byte{}, io.EOF
		}

		record := trimRecordEnding(reader.recordBuffer)
		if len(record) == 0 {
			if errors.Is(readErr, io.EOF) {
				return [csvFieldCount][]byte{}, io.EOF
			}
			reader.recordBuffer = reader.recordBuffer[:0]
			continue
		}
		fields, parseErr := reader.parseRecord(record)
		if errors.Is(parseErr, errUnterminatedQuotedField) && !errors.Is(readErr, io.EOF) {
			continue
		}
		return fields, parseErr
	}
}

func (reader *csvBytesReader) parseRecord(record []byte) ([csvFieldCount][]byte, error) {
	var fields [csvFieldCount][]byte
	reader.quotedFields = reader.quotedFields[:0]
	fieldIndex := 0
	offset := 0

	for {
		if fieldIndex == csvFieldCount {
			return fields, fmt.Errorf("record contains more than %d fields", csvFieldCount)
		}

		if offset < len(record) && record[offset] == '"' {
			quotedStart := len(reader.quotedFields)
			offset++
			closed := false
			for offset < len(record) {
				if record[offset] == '\r' && offset+1 < len(record) && record[offset+1] == '\n' {
					reader.quotedFields = append(reader.quotedFields, '\n')
					offset += 2
					continue
				}
				if record[offset] != '"' {
					reader.quotedFields = append(reader.quotedFields, record[offset])
					offset++
					continue
				}
				if offset+1 < len(record) && record[offset+1] == '"' {
					reader.quotedFields = append(reader.quotedFields, '"')
					offset += 2
					continue
				}
				offset++
				closed = true
				break
			}
			if !closed {
				return fields, errUnterminatedQuotedField
			}
			if offset < len(record) && record[offset] != ',' {
				return fields, fmt.Errorf("unexpected character after closing quote")
			}
			fields[fieldIndex] = reader.quotedFields[quotedStart:]
		} else {
			fieldStart := offset
			for offset < len(record) && record[offset] != ',' {
				if record[offset] == '"' {
					return fields, fmt.Errorf("unexpected quote in unquoted field")
				}
				offset++
			}
			fields[fieldIndex] = record[fieldStart:offset]
		}

		fieldIndex++
		if offset == len(record) {
			break
		}
		offset++
	}

	if fieldIndex != csvFieldCount {
		return fields, fmt.Errorf("record contains %d fields, expected %d", fieldIndex, csvFieldCount)
	}
	return fields, nil
}

func trimRecordEnding(record []byte) []byte {
	if len(record) > 0 && record[len(record)-1] == '\n' {
		record = record[:len(record)-1]
		if len(record) > 0 && record[len(record)-1] == '\r' {
			record = record[:len(record)-1]
		}
	}
	return record
}
