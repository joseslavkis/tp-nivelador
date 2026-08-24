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

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyID   uint32
	InputFile  string
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	const mainAction = "process-input-file"
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

	reader := csv.NewReader(inputFile)
	reader.FieldsPerRecord = 5
	if err := client.sendBets(reader); err != nil {
		return err
	}

	writer := csv.NewWriter(outputFile)
	if err := client.receiveWinners(writer); err != nil {
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyID)

	return nil
}

func (client *Client) sendBets(reader *csv.Reader) error {
	for messageID := 0; ; messageID++ {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read input record %d: %w", messageID, err)
		}

		bet, err := client.parseBet(record)
		if err != nil {
			return fmt.Errorf("parse input record %d: %w", messageID, err)
		}
		payload, err := protocol.EncodeBet(bet)
		if err != nil {
			return fmt.Errorf("encode input record %d: %w", messageID, err)
		}

		if err := safe_socket.SendMessage(client.conn, safe_socket.Message{
			Type:    safe_socket.MessageTypeBet,
			Payload: payload,
		}); err != nil {
			return fmt.Errorf("send input record %d: %w", messageID, err)
		}
	}

	if err := safe_socket.SendMessage(client.conn, safe_socket.Message{
		Type: safe_socket.MessageTypeEnd,
	}); err != nil {
		return fmt.Errorf("send end of bets: %w", err)
	}
	return nil
}

func (client *Client) parseBet(record []string) (protocol.BetPayload, error) {
	document, err := strconv.ParseUint(record[2], 10, 64)
	if err != nil {
		return protocol.BetPayload{}, fmt.Errorf("invalid document %q: %w", record[2], err)
	}
	number, err := strconv.ParseUint(record[4], 10, 32)
	if err != nil {
		return protocol.BetPayload{}, fmt.Errorf("invalid number %q: %w", record[4], err)
	}

	return protocol.BetPayload{
		AgencyID:  client.config.AgencyID,
		FirstName: record[0],
		LastName:  record[1],
		Document:  document,
		Birthdate: record[3],
		Number:    uint32(number),
	}, nil
}

func (client *Client) receiveWinners(writer *csv.Writer) error {
	for {
		message, err := safe_socket.RecvMessage(client.conn)
		if err != nil {
			return fmt.Errorf("receive server response: %w", err)
		}

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
