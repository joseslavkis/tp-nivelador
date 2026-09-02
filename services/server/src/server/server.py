import os
import socket
import tempfile
import threading
from collections.abc import Callable

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
        session = _ClientSession(
            client_socket=client_socket,
            storage_directory=self.storage_directory,
            wait_for_quorum=self._wait_for_quorum,
            raise_if_shutdown=self._raise_if_shutdown,
        )
        session.handle()

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

    def run(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()

            if not self._register_server_socket(server_socket):
                return

            try:
                self._accept_connections(server_socket)
            finally:
                self._finish_run()

    def _register_server_socket(self, server_socket: socket.socket) -> bool:
        with self._state_lock:
            if self._shutdown_event.is_set():
                return False
            self._server_socket = server_socket
        return True

    def _accept_connections(self, server_socket: socket.socket) -> None:
        while not self._shutdown_event.is_set():
            self._reap_client_threads()
            accepted_client = self._accept_client(server_socket)
            if accepted_client is None:
                break

            client_socket, client_address = accepted_client
            if not self._register_client_socket(client_socket):
                break
            self._start_client_worker(client_socket, client_address)

    def _accept_client(
        self, server_socket: socket.socket
    ) -> tuple[socket.socket, tuple[str, int]] | None:
        action = "accept-connection"
        try:
            logger.info(action, logger.LogResult.in_progress)
            accepted_client = server_socket.accept()
        except OSError as error:
            if self._shutdown_event.is_set():
                return None
            logger.error(action, logger.LogResult.fail, "err", error)
            raise

        logger.info(action, logger.LogResult.success)
        return accepted_client

    def _register_client_socket(self, client_socket: socket.socket) -> bool:
        with self._state_lock:
            if self._shutdown_event.is_set():
                client_socket.close()
                return False
            self._client_sockets.add(client_socket)
        return True

    def _start_client_worker(
        self,
        client_socket: socket.socket,
        client_address: tuple[str, int],
    ) -> None:
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

    def _finish_run(self) -> None:
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


class _ClientSession:
    def __init__(
        self,
        client_socket: socket.socket,
        storage_directory: str,
        wait_for_quorum: Callable[[int], bool],
        raise_if_shutdown: Callable[[], None],
    ) -> None:
        self._client_socket = client_socket
        self._storage_directory = storage_directory
        self._wait_for_quorum = wait_for_quorum
        self._raise_if_shutdown = raise_if_shutdown
        self._agency_id: int | None = None
        self._bet_amount = 0

    def handle(self) -> None:
        action = "handle-client"
        with tempfile.TemporaryDirectory(
            prefix="lottery-session-", dir=self._storage_directory
        ) as session_directory:
            lottery = Lottery(os.path.join(session_directory, "bets.csv"))

            try:
                logger.info(action, logger.LogResult.in_progress)
                session_completed = self._receive_client_messages(lottery)
                if not session_completed:
                    return
                logger.info(
                    action,
                    logger.LogResult.success,
                    "bets-amount",
                    self._bet_amount,
                )
            except ValueError as error:
                self._report_client_error(action, error)
                raise ClientProtocolError(str(error)) from error
            except ClientStorageError as error:
                self._report_client_error(action, error)
                raise

    def _receive_client_messages(self, lottery: Lottery) -> bool:
        while True:
            self._raise_if_shutdown()
            message_type, payload = safe_socket.recv_message(self._client_socket)

            if message_type == protocol.MESSAGE_TYPE_BETS_BATCH:
                self._handle_bets_batch(lottery, payload)
                continue
            if message_type == protocol.MESSAGE_TYPE_END:
                return self._handle_client_end(lottery, payload)

            raise ValueError(f"unexpected client message type: {message_type}")

    def _handle_bets_batch(self, lottery: Lottery, payload: bytes) -> None:
        batch_agency_id, batch_size = self._store_bet_batch(lottery, payload)
        self._agency_id = batch_agency_id
        self._bet_amount += batch_size
        safe_socket.send_message(
            self._client_socket,
            protocol.MESSAGE_TYPE_BATCH_ACK,
            b"",
        )

    def _handle_client_end(self, lottery: Lottery, payload: bytes) -> bool:
        end_agency_id = protocol.decode_agency_id(payload)
        if self._agency_id is not None and self._agency_id != end_agency_id:
            raise ValueError("end agency id must match batch agency id")

        self._agency_id = end_agency_id
        if not self._wait_for_quorum(end_agency_id):
            return False

        self._send_winners(lottery)
        return True

    def _store_bet_batch(
        self, lottery: Lottery, payload: bytes
    ) -> tuple[int, int]:
        bets: list[Bet] = []
        batch_agency_id: int | None = None
        bet_count = 0
        remaining_bet_capacity = MAX_BETS_PER_SESSION - self._bet_amount

        for bet_payload in protocol.iter_bet_batch(payload):
            self._raise_if_shutdown()
            if bet_count == remaining_bet_capacity:
                raise ValueError(
                    f"session exceeds {MAX_BETS_PER_SESSION} bets"
                )
            batch_agency_id = self._resolve_batch_agency_id(
                bet_payload.agency_id, batch_agency_id
            )

            self._append_bet(lottery, bets, bet_payload)
            bet_count += 1

        if bets:
            self._store_bets(lottery, bets)
        if batch_agency_id is None:
            raise ValueError("bet batch cannot be empty")

        return batch_agency_id, bet_count

    def _resolve_batch_agency_id(
        self, agency_id: int, batch_agency_id: int | None
    ) -> int:
        if batch_agency_id is None:
            batch_agency_id = agency_id
        elif agency_id != batch_agency_id:
            raise ValueError("all bets in a batch must use one agency id")

        if self._agency_id is not None and agency_id != self._agency_id:
            raise ValueError("all bets in a connection must use one agency id")
        return batch_agency_id

    def _append_bet(
        self,
        lottery: Lottery,
        bets: list[Bet],
        bet_payload: protocol.BetPayload,
    ) -> None:
        bets.append(self._to_domain_bet(bet_payload))
        if len(bets) == BET_STORAGE_CHUNK_SIZE:
            self._store_bets(lottery, bets)
            bets.clear()

    def _send_winners(self, lottery: Lottery) -> None:
        if self._bet_amount > 0:
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
                        self._client_socket,
                        protocol.MESSAGE_TYPE_WINNER,
                        protocol.encode_bet(self._to_bet_payload(bet)),
                    )

        safe_socket.send_message(
            self._client_socket, protocol.MESSAGE_TYPE_END, b""
        )

    def _report_client_error(self, action: str, error: Exception) -> None:
        self._send_protocol_error(error)
        logger.error(
            action,
            logger.LogResult.fail,
            "bets-amount",
            self._bet_amount,
            "err",
            error,
        )

    def _send_protocol_error(self, error: Exception) -> None:
        try:
            safe_socket.send_message(
                self._client_socket,
                protocol.MESSAGE_TYPE_ERROR,
                str(error).encode("utf-8"),
            )
        except OSError:
            pass

    @staticmethod
    def _store_bets(lottery: Lottery, bets: list[Bet]) -> None:
        try:
            lottery.store_bets(bets)
        except OSError as error:
            raise ClientStorageError("failed to store bets") from error

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
