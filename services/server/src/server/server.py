import os
import socket
import tempfile
import threading

import logger
import protocol
import safe_socket
from lottery import Bet, Lottery

MAX_BETS_PER_SESSION = 1_000_000
BET_STORAGE_CHUNK_SIZE = 1024


class ClientProtocolError(Exception):
    pass


class ClientStorageError(Exception):
    pass


class ServerShutdown(Exception):
    pass


class Server:
    def __init__(
        self,
        server_host: str,
        server_port: int,
        storage_directory: str,
        agency_quorum_min: int,
    ) -> None:
        if agency_quorum_min <= 0:
            raise ValueError("AGENCY_QUORUM_MIN must be a positive integer")

        self.server_host = server_host
        self.server_port = server_port
        self.storage_directory = storage_directory
        self._agency_quorum_min = agency_quorum_min
        self._completed_agencies: set[int] = set()
        self._quorum_condition = threading.Condition()
        self._shutdown_event = threading.Event()
        self._state_lock = threading.Lock()
        self._server_socket: socket.socket | None = None
        self._client_sockets: set[socket.socket] = set()
        self._client_threads: set[threading.Thread] = set()
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
                    self._raise_if_shutdown()
                    message_type, payload = safe_socket.recv_message(client_socket)

                    if message_type == protocol.MESSAGE_TYPE_BETS_BATCH:
                        batch_agency_id, batch_size = self._store_bet_batch(
                            lottery,
                            payload,
                            agency_id,
                            MAX_BETS_PER_SESSION - bet_amount,
                        )
                        agency_id = batch_agency_id
                        bet_amount += batch_size
                        safe_socket.send_message(
                            client_socket,
                            protocol.MESSAGE_TYPE_BATCH_ACK,
                            b"",
                        )
                        continue

                    if message_type == protocol.MESSAGE_TYPE_END:
                        end_agency_id = protocol.decode_agency_id(payload)
                        if agency_id is not None and agency_id != end_agency_id:
                            raise ValueError(
                                "end agency id must match batch agency id"
                            )
                        agency_id = end_agency_id
                        if not self._wait_for_quorum(agency_id):
                            return
                        self._send_winners(
                            client_socket, agency_id, bet_amount, lottery
                        )
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

    def _wait_for_quorum(self, agency_id: int) -> bool:
        with self._quorum_condition:
            self._completed_agencies.add(agency_id)
            if len(self._completed_agencies) >= self._agency_quorum_min:
                self._quorum_condition.notify_all()

            self._quorum_condition.wait_for(
                lambda: len(self._completed_agencies)
                >= self._agency_quorum_min
                or self._shutdown_event.is_set()
            )
            completed_agency_count = len(self._completed_agencies)

        if self._shutdown_event.is_set():
            return False

        logger.info(
            "wait-agency-quorum",
            logger.LogResult.success,
            "agency-id",
            agency_id,
            "completed-agencies",
            completed_agency_count,
            "required-agencies",
            self._agency_quorum_min,
        )
        return True

    def _store_bet_batch(
        self,
        lottery: Lottery,
        payload: bytes,
        session_agency_id: int | None,
        remaining_bet_capacity: int,
    ) -> tuple[int, int]:
        bets: list[Bet] = []
        batch_agency_id: int | None = None
        bet_count = 0

        for bet_payload in protocol.iter_bet_batch(payload):
            self._raise_if_shutdown()
            if bet_count == remaining_bet_capacity:
                raise ValueError(
                    f"session exceeds {MAX_BETS_PER_SESSION} bets"
                )
            if batch_agency_id is None:
                batch_agency_id = bet_payload.agency_id
            elif bet_payload.agency_id != batch_agency_id:
                raise ValueError("all bets in a batch must use one agency id")
            if (
                session_agency_id is not None
                and bet_payload.agency_id != session_agency_id
            ):
                raise ValueError(
                    "all bets in a connection must use one agency id"
                )

            bets.append(self._to_domain_bet(bet_payload))
            bet_count += 1
            if len(bets) == BET_STORAGE_CHUNK_SIZE:
                self._store_bets(lottery, bets)
                bets.clear()

        if bets:
            self._store_bets(lottery, bets)
        if batch_agency_id is None:
            raise ValueError("bet batch cannot be empty")

        return batch_agency_id, bet_count

    @staticmethod
    def _store_bets(lottery: Lottery, bets: list[Bet]) -> None:
        try:
            lottery.store_bets(bets)
        except OSError as error:
            raise ClientStorageError("failed to store bets") from error

    def _send_winners(
        self,
        client_socket: socket.socket,
        agency_id: int,
        bet_amount: int,
        lottery: Lottery,
    ) -> None:
        if bet_amount > 0:
            stored_bets = iter(lottery.load_bets())
            while True:
                self._raise_if_shutdown()
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
            with self._state_lock:
                if self._shutdown_event.is_set():
                    return
                self._server_socket = server_socket

            try:
                while not self._shutdown_event.is_set():
                    self._reap_client_threads()
                    try:
                        logger.info(action, logger.LogResult.in_progress)
                        client_socket, client_address = server_socket.accept()
                    except OSError as error:
                        if self._shutdown_event.is_set():
                            break
                        logger.error(action, logger.LogResult.fail, "err", error)
                        raise
                    logger.info(action, logger.LogResult.success)

                    with self._state_lock:
                        if self._shutdown_event.is_set():
                            client_socket.close()
                            break
                        self._client_sockets.add(client_socket)

                    client_thread = threading.Thread(
                        target=self._handle_client_connection,
                        args=(client_socket,),
                        name=f"client-{client_address[0]}:{client_address[1]}",
                        daemon=False,
                    )
                    try:
                        client_thread.start()
                    except RuntimeError:
                        with self._state_lock:
                            self._client_sockets.discard(client_socket)
                        client_socket.close()
                        raise
                    with self._state_lock:
                        self._client_threads.add(client_thread)
            finally:
                self.shutdown()
                with self._state_lock:
                    client_threads = tuple(self._client_threads)
                for client_thread in client_threads:
                    client_thread.join()
                with self._state_lock:
                    self._server_socket = None
                    self._client_threads.clear()

    def _reap_client_threads(self) -> None:
        with self._state_lock:
            completed_threads = tuple(
                thread
                for thread in self._client_threads
                if not thread.is_alive()
            )
            self._client_threads.difference_update(completed_threads)

        for client_thread in completed_threads:
            client_thread.join()

    def shutdown(self) -> None:
        self.request_shutdown()
        with self._state_lock:
            client_sockets = tuple(self._client_sockets)

        for client_socket in client_sockets:
            self._close_socket(client_socket)

        with self._quorum_condition:
            self._quorum_condition.notify_all()

    def request_shutdown(self) -> None:
        self._shutdown_event.set()
        server_socket = self._server_socket
        if server_socket is not None:
            self._close_socket(server_socket)

    def _raise_if_shutdown(self) -> None:
        if self._shutdown_event.is_set():
            raise ServerShutdown

    @staticmethod
    def _close_socket(sock: socket.socket) -> None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass

    def _handle_client_connection(self, client_socket: socket.socket) -> None:
        try:
            with client_socket:
                self._handle_client(client_socket)
        except (
            ClientProtocolError,
            ClientStorageError,
            ConnectionError,
            ServerShutdown,
        ):
            return
        except OSError:
            if not self._shutdown_event.is_set():
                raise
        finally:
            with self._state_lock:
                self._client_sockets.discard(client_socket)
