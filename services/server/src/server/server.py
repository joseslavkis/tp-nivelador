import os
import socket
import tempfile

import logger
import protocol
import safe_socket
from lottery import Bet, Lottery

MAX_BETS_PER_SESSION = 1_000_000


class ClientProtocolError(Exception):
    pass


class ClientStorageError(Exception):
    pass


class Server:
    def __init__(
        self, server_host: str, server_port: int, storage_directory: str
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.storage_directory = storage_directory
        os.makedirs(storage_directory, exist_ok=True)

    def _handle_client(self, client_socket: socket.socket) -> None:
        action = "handle-client"
        bet_amount = 0
        agency_id: int | None = None

        with tempfile.TemporaryDirectory(
            prefix="lottery-session-", dir=self.storage_directory
        ) as session_directory:
            lottery = Lottery(os.path.join(session_directory, "bets.csv"))

            try:
                logger.info(action, logger.LogResult.in_progress)
                while True:
                    message_type, payload = safe_socket.recv_message(client_socket)

                    if message_type == protocol.MESSAGE_TYPE_BETS_BATCH:
                        bet_payloads = protocol.decode_bet_batch(payload)
                        if (
                            len(bet_payloads)
                            > MAX_BETS_PER_SESSION - bet_amount
                        ):
                            raise ValueError(
                                f"session exceeds {MAX_BETS_PER_SESSION} bets"
                            )

                        batch_agency_id = bet_payloads[0].agency_id
                        if any(
                            bet.agency_id != batch_agency_id
                            for bet in bet_payloads
                        ):
                            raise ValueError(
                                "all bets in a batch must use one agency id"
                            )
                        if agency_id is not None and agency_id != batch_agency_id:
                            raise ValueError(
                                "all bets in a connection must use one agency id"
                            )

                        self._store_bets(
                            lottery,
                            [
                                self._to_domain_bet(bet)
                                for bet in bet_payloads
                            ],
                        )
                        agency_id = batch_agency_id
                        bet_amount += len(bet_payloads)
                        safe_socket.send_message(
                            client_socket,
                            protocol.MESSAGE_TYPE_BATCH_ACK,
                            b"",
                        )
                        continue

                    if message_type == protocol.MESSAGE_TYPE_END:
                        if payload:
                            raise ValueError("end message must have an empty payload")
                        self._send_winners(client_socket, agency_id, lottery)
                        logger.info(
                            action,
                            logger.LogResult.success,
                            "bets-amount",
                            bet_amount,
                        )
                        return

                    raise ValueError(
                        f"unexpected client message type: {message_type}"
                    )
            except ValueError as error:
                self._send_protocol_error(client_socket, error)
                logger.error(
                    action,
                    logger.LogResult.fail,
                    "bets-amount",
                    bet_amount,
                    "err",
                    error,
                )
                raise ClientProtocolError(str(error)) from error
            except ClientStorageError as error:
                self._send_protocol_error(client_socket, error)
                logger.error(
                    action,
                    logger.LogResult.fail,
                    "bets-amount",
                    bet_amount,
                    "err",
                    error,
                )
                raise

    @staticmethod
    def _store_bets(lottery: Lottery, bets: list[Bet]) -> None:
        try:
            lottery.store_bets(bets)
        except OSError as error:
            raise ClientStorageError("failed to store bets") from error

    def _send_winners(
        self, client_socket: socket.socket, agency_id: int | None, lottery: Lottery
    ) -> None:
        if agency_id is not None:
            stored_bets = iter(lottery.load_bets())
            while True:
                try:
                    bet = next(stored_bets)
                except StopIteration:
                    break
                except OSError as error:
                    raise ClientStorageError("failed to load bets") from error

                if lottery.has_won(bet):
                    safe_socket.send_message(
                        client_socket,
                        protocol.MESSAGE_TYPE_WINNER,
                        protocol.encode_bet(self._to_bet_payload(bet)),
                    )

        safe_socket.send_message(client_socket, protocol.MESSAGE_TYPE_END, b"")

    @staticmethod
    def _send_protocol_error(
        client_socket: socket.socket, error: Exception
    ) -> None:
        try:
            safe_socket.send_message(
                client_socket,
                protocol.MESSAGE_TYPE_ERROR,
                str(error).encode("utf-8"),
            )
        except OSError:
            pass

    @staticmethod
    def _to_domain_bet(bet: protocol.BetPayload) -> Bet:
        return Bet(
            agency_id=bet.agency_id,
            first_name=bet.first_name,
            last_name=bet.last_name,
            document=bet.document,
            birthdate=bet.birthdate,
            number=bet.number,
        )

    @staticmethod
    def _to_bet_payload(bet: Bet) -> protocol.BetPayload:
        return protocol.BetPayload(
            agency_id=bet.agency_id,
            first_name=bet.first_name,
            last_name=bet.last_name,
            document=bet.document,
            birthdate=bet.birthdate,
            number=bet.number,
        )

    def run(self) -> None:
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except OSError as error:
                    logger.error(action, logger.LogResult.fail, "err", error)
                    raise
                logger.info(action, logger.LogResult.success)

                try:
                    with client_socket:
                        self._handle_client(client_socket)
                except (ClientProtocolError, ClientStorageError, ConnectionError):
                    continue
