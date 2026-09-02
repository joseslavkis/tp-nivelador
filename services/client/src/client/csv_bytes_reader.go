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

type csvBytesReader struct {
	reader       *bufio.Reader
	recordBuffer []byte
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
		return reader.parseRecord(record)
	}
}

func (reader *csvBytesReader) parseRecord(record []byte) ([csvFieldCount][]byte, error) {
	var fields [csvFieldCount][]byte
	fieldIndex := 0
	fieldStart := 0

	for offset, value := range record {
		if value != ',' {
			continue
		}
		if fieldIndex == csvFieldCount-1 {
			return fields, fmt.Errorf("record contains more than %d fields", csvFieldCount)
		}
		fields[fieldIndex] = record[fieldStart:offset]
		fieldIndex++
		fieldStart = offset + 1
	}
	fields[fieldIndex] = record[fieldStart:]
	fieldIndex++

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
