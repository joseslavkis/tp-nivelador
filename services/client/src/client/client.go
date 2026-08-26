package client

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const connectToServerAction = "connect-to-server"
const processInputFileAction = "process-input-file"

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyID   uint32
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn           net.Conn
	config         ClientConfig
	messageHeader  []byte
	receivePayload []byte
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{
		conn:          conn,
		config:        config,
		messageHeader: make([]byte, safe_socket.MessageHeaderSize),
	}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	var err error
	var conn net.Conn

	logger.Info(connectToServerAction, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(connectToServerAction, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(connectToServerAction, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	defer client.conn.Close()

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	reader := newCSVBytesReader(inputFile)
	if err := client.sendBets(reader); err != nil {
		return err
	}

	writer := csv.NewWriter(outputFile)
	if err := client.receiveWinners(writer); err != nil {
		return err
	}

	logger.Info(processInputFileAction, logger.Success, "agency-id", client.config.AgencyID)

	return nil
}

func (client *Client) sendBets(reader *csvBytesReader) error {
	var encoder protocol.BetBatchEncoder
	encoder.Reset(nil)
	batchID := 0
	recordID := 0
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read input record %d: %w", recordID, err)
		}

		document, number, err := parseBetNumbers(record)
		if err != nil {
			return fmt.Errorf("parse input record %d: %w", recordID, err)
		}
		if err := encoder.Append(
			client.config.AgencyID,
			record[0],
			record[1],
			document,
			record[3],
			number,
		); err != nil {
			return fmt.Errorf("encode input record %d: %w", recordID, err)
		}
		recordID++
		if encoder.Count() < client.config.BatchSize {
			continue
		}

		payload, err := encoder.Payload()
		if err != nil {
			return fmt.Errorf("encode bet batch %d: %w", batchID, err)
		}
		if err := client.sendBetBatch(payload, batchID); err != nil {
			return err
		}
		encoder.Reset(payload)
		batchID++
	}

	if encoder.Count() > 0 {
		payload, err := encoder.Payload()
		if err != nil {
			return fmt.Errorf("encode bet batch %d: %w", batchID, err)
		}
		if err := client.sendBetBatch(payload, batchID); err != nil {
			return err
		}
	}

	if err := safe_socket.SendMessageWithHeader(client.conn, safe_socket.Message{
		Type: safe_socket.MessageTypeEnd,
	}, client.messageHeader); err != nil {
		return fmt.Errorf("send end of bets: %w", err)
	}
	return nil
}

func (client *Client) sendBetBatch(payload []byte, batchID int) error {
	if err := safe_socket.SendMessageWithHeader(client.conn, safe_socket.Message{
		Type:    safe_socket.MessageTypeBetsBatch,
		Payload: payload,
	}, client.messageHeader); err != nil {
		return fmt.Errorf("send bet batch %d: %w", batchID, err)
	}

	response, err := safe_socket.RecvMessageInto(
		client.conn,
		client.messageHeader,
		client.receivePayload,
	)
	if err != nil {
		return fmt.Errorf("receive acknowledgment for bet batch %d: %w", batchID, err)
	}
	client.receivePayload = response.Payload
	switch response.Type {
	case safe_socket.MessageTypeBatchAck:
		if len(response.Payload) != 0 {
			return fmt.Errorf("acknowledgment for bet batch %d must have an empty payload", batchID)
		}
		return nil
	case safe_socket.MessageTypeError:
		return fmt.Errorf("server rejected bet batch %d: %s", batchID, response.Payload)
	default:
		return fmt.Errorf(
			"unexpected response type %d for bet batch %d",
			response.Type,
			batchID,
		)
	}
}

func parseBetNumbers(record [csvFieldCount][]byte) (uint64, uint32, error) {
	document, err := strconv.ParseUint(string(record[2]), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid document %q: %w", record[2], err)
	}
	number, err := strconv.ParseUint(string(record[4]), 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid number %q: %w", record[4], err)
	}
	return document, uint32(number), nil
}

func (client *Client) receiveWinners(writer *csv.Writer) error {
	for {
		message, err := safe_socket.RecvMessageInto(
			client.conn,
			client.messageHeader,
			client.receivePayload,
		)
		if err != nil {
			return fmt.Errorf("receive server response: %w", err)
		}
		client.receivePayload = message.Payload

		switch message.Type {
		case safe_socket.MessageTypeWinner:
			winner, err := protocol.DecodeBet(message.Payload)
			if err != nil {
				return fmt.Errorf("decode winner: %w", err)
			}
			if winner.AgencyID != client.config.AgencyID {
				return fmt.Errorf("received winner for agency %d", winner.AgencyID)
			}
			if err := writer.Write([]string{
				winner.FirstName,
				winner.LastName,
				strconv.FormatUint(winner.Document, 10),
				winner.Birthdate,
				strconv.FormatUint(uint64(winner.Number), 10),
			}); err != nil {
				return fmt.Errorf("write winner: %w", err)
			}
		case safe_socket.MessageTypeEnd:
			if len(message.Payload) != 0 {
				return fmt.Errorf("end message must have an empty payload")
			}
			writer.Flush()
			if err := writer.Error(); err != nil {
				return fmt.Errorf("flush winners: %w", err)
			}
			return nil
		case safe_socket.MessageTypeError:
			return fmt.Errorf("server rejected request: %s", message.Payload)
		default:
			return fmt.Errorf("unexpected server message type %d", message.Type)
		}
	}
}
