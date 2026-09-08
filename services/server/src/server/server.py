import os
import socket
import threading
from dataclasses import dataclass, field

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


@dataclass
class _RoundState:
    round_id: int
    admitted_workers: int = 0
    active_workers: int = 0
    agencies: set[int] = field(default_factory=set)
    completed_agencies: set[int] = field(default_factory=set)
    quorum_reached: bool = False
    aborted: bool = False
    closing: bool = False


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

        self._quorum_condition = threading.Condition()
        self._round_counter = 0
        self._current_round = _RoundState(self._round_counter)

        self._shutdown_event = threading.Event()
        self._state_lock = threading.Lock()
        self._server_socket: socket.socket | None = None
        self._client_sockets: set[socket.socket] = set()
        self._client_threads: set[threading.Thread] = set()

        os.makedirs(storage_directory, exist_ok=True)
        self._lottery = Lottery(os.path.join(storage_directory, "bets.csv"))
        self._lottery_lock = threading.Lock()

        with self._lottery_lock:
            self._clear_lottery_storage_locked()

    def _handle_client(self, client_socket: socket.socket) -> None:
        round_state = self._register_round_worker()
        _ClientSession(client_socket, self, round_state).handle()

    def _register_round_worker(self) -> _RoundState:
        with self._quorum_condition:
            self._quorum_condition.wait_for(
                lambda: self._shutdown_event.is_set()
                or (
                    self._current_round is not None
                    and not self._current_round.aborted
                    and not self._current_round.closing
                    and self._current_round.admitted_workers < self._agency_quorum_min
                )
            )
            self._raise_if_shutdown()

            round_state = self._current_round
            if round_state is None:
                raise ServerShutdown

            round_state.admitted_workers += 1
            round_state.active_workers += 1
            return round_state

    def _register_round_agency(
        self, round_state: _RoundState, agency_id: int
    ) -> None:
        with self._quorum_condition:
            self._raise_if_shutdown()

            if self._current_round is not round_state or round_state.aborted:
                raise ValueError("round is no longer active")

            if agency_id in round_state.agencies:
                round_state.aborted = True
                self._quorum_condition.notify_all()
                raise ValueError("agency already participates in the current round")

            round_state.agencies.add(agency_id)

    def _wait_for_quorum(self, round_state: _RoundState, agency_id: int) -> bool:
        with self._quorum_condition:
            self._raise_if_shutdown()

            if self._current_round is not round_state or round_state.aborted:
                return False

            if agency_id not in round_state.agencies:
                raise ValueError("agency was not registered in the current round")

            if agency_id in round_state.completed_agencies:
                round_state.aborted = True
                self._quorum_condition.notify_all()
                raise ValueError("agency already finalized the current round")

            round_state.completed_agencies.add(agency_id)

            if len(round_state.completed_agencies) == self._agency_quorum_min:
                round_state.quorum_reached = True
                self._quorum_condition.notify_all()

            self._quorum_condition.wait_for(
                lambda: self._shutdown_event.is_set()
                or round_state.aborted
                or round_state.quorum_reached
            )

            if self._shutdown_event.is_set() or round_state.aborted:
                return False

            completed_agency_count = len(round_state.completed_agencies)

        logger.info(
            "wait-agency-quorum",
            logger.LogResult.success,
            "agency-id",
            agency_id,
            "completed-agencies",
            completed_agency_count,
            "required-agencies",
            self._agency_quorum_min,
            "round-id",
            round_state.round_id,
        )
        return True

    def _release_round_worker(
        self, round_state: _RoundState, successful: bool
    ) -> None:
        with self._quorum_condition:
            if round_state.active_workers <= 0:
                return

            if not successful and not self._shutdown_event.is_set():
                round_state.aborted = True
                self._quorum_condition.notify_all()

            round_state.active_workers -= 1
            should_close_round = (
                round_state.active_workers == 0 and not round_state.closing
            )
            if should_close_round:
                round_state.closing = True

        if not should_close_round:
            return

        cleanup_error = None
        try:
            with self._lottery_lock:
                self._clear_lottery_storage_locked()
        except ClientStorageError as error:
            cleanup_error = error

        with self._quorum_condition:
            if self._current_round is round_state:
                self._current_round = None
                if cleanup_error is None and not self._shutdown_event.is_set():
                    self._round_counter += 1
                    self._current_round = _RoundState(self._round_counter)
            self._quorum_condition.notify_all()

        if cleanup_error is not None:
            self.request_shutdown()
            raise cleanup_error

        logger.info(
            "round-closed",
            logger.LogResult.success,
            "round-id",
            round_state.round_id,
        )

    def _clear_lottery_storage_locked(self) -> None:
        try:
            with open(self._lottery.storage_path, "w"):
                pass
        except OSError as error:
            raise ClientStorageError("failed to clear bet storage") from error

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
        self, client_socket: socket.socket, client_address: tuple[str, int]
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
                thread for thread in self._client_threads if not thread.is_alive()
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
        server: Server,
        round_state: _RoundState,
    ) -> None:
        self._client_socket = client_socket
        self._server = server
        self._round_state = round_state
        self._agency_id: int | None = None
        self._round_agency_registered = False
        self._bet_amount = 0

    def handle(self) -> None:
        action = "handle-client"
        successful = False

        try:
            logger.info(action, logger.LogResult.in_progress)
            if not self._receive_client_messages():
                return

            successful = True
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
        finally:
            self._server._release_round_worker(self._round_state, successful)

    def _receive_client_messages(self) -> bool:
        while True:
            self._server._raise_if_shutdown()
            message_type, payload = safe_socket.recv_message(self._client_socket)

            if message_type == protocol.MESSAGE_TYPE_BETS_BATCH:
                self._handle_bets_batch(payload)
            elif message_type == protocol.MESSAGE_TYPE_END:
                return self._handle_client_end(payload)
            else:
                raise ValueError(f"unexpected client message type: {message_type}")

    def _handle_bets_batch(self, payload: bytes) -> None:
        self._prepare_batch_agency(payload)
        _, batch_size = self._store_bet_batch(payload)
        self._bet_amount += batch_size
        safe_socket.send_message(
            self._client_socket,
            protocol.MESSAGE_TYPE_BATCH_ACK,
            b"",
        )

    def _prepare_batch_agency(self, payload: bytes) -> None:
        try:
            first_bet = next(protocol.iter_bet_batch(payload))
        except StopIteration as error:
            raise ValueError("bet batch cannot be empty") from error

        agency_id = first_bet.agency_id
        if self._agency_id is not None and self._agency_id != agency_id:
            raise ValueError("all bets in a connection must use one agency id")

        if not self._round_agency_registered:
            self._server._register_round_agency(self._round_state, agency_id)
            self._round_agency_registered = True
            self._agency_id = agency_id

    def _handle_client_end(self, payload: bytes) -> bool:
        end_agency_id = protocol.decode_agency_id(payload)

        if self._agency_id is not None and self._agency_id != end_agency_id:
            raise ValueError("end agency id must match batch agency id")

        if not self._round_agency_registered:
            self._server._register_round_agency(self._round_state, end_agency_id)
            self._round_agency_registered = True
            self._agency_id = end_agency_id

        if not self._server._wait_for_quorum(self._round_state, end_agency_id):
            return False

        self._send_winners()
        return True

    def _store_bet_batch(self, payload: bytes) -> tuple[int, int]:
        with self._server._lottery_lock:
            self._server._raise_if_shutdown()
            previous_storage_size = self._lottery_storage_size()

            try:
                return self._store_bet_batch_locked(payload)
            except Exception:
                self._rollback_bet_storage(previous_storage_size)
                raise

    def _store_bet_batch_locked(self, payload: bytes) -> tuple[int, int]:
        bets: list[Bet] = []
        batch_agency_id: int | None = None
        bet_count = 0
        remaining_bet_capacity = MAX_BETS_PER_SESSION - self._bet_amount

        for bet_payload in protocol.iter_bet_batch(payload):
            self._server._raise_if_shutdown()

            if bet_count == remaining_bet_capacity:
                raise ValueError(f"session exceeds {MAX_BETS_PER_SESSION} bets")

            batch_agency_id = self._resolve_batch_agency_id(
                bet_payload.agency_id, batch_agency_id
            )
            bets.append(self._to_domain_bet(bet_payload))
            bet_count += 1

            if len(bets) == BET_STORAGE_CHUNK_SIZE:
                self._store_bets_locked(bets)
                bets.clear()

        if bets:
            self._store_bets_locked(bets)

        if batch_agency_id is None:
            raise ValueError("bet batch cannot be empty")

        self._server._raise_if_shutdown()
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

    def _send_winners(self) -> None:
        for bet in self._load_winners():
            self._server._raise_if_shutdown()
            safe_socket.send_message(
                self._client_socket,
                protocol.MESSAGE_TYPE_WINNER,
                protocol.encode_bet(self._to_bet_payload(bet)),
            )

        safe_socket.send_message(
            self._client_socket,
            protocol.MESSAGE_TYPE_END,
            b"",
        )

    def _load_winners(self) -> list[Bet]:
        if self._bet_amount == 0:
            return []

        winners = []
        try:
            with self._server._lottery_lock:
                for bet in self._server._lottery.load_bets():
                    self._server._raise_if_shutdown()
                    if (bet.agency_id == self._agency_id
                        and self._server._lottery.has_won(bet)
                    ):
                        winners.append(bet)
        except OSError as error:
            raise ClientStorageError("failed to load bets") from error

        return winners

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

    def _store_bets_locked(self, bets: list[Bet]) -> None:
        try:
            self._server._lottery.store_bets(bets)
        except OSError as error:
            raise ClientStorageError("failed to store bets") from error

    def _lottery_storage_size(self) -> int:
        try:
            return os.path.getsize(self._server._lottery.storage_path)
        except FileNotFoundError:
            return 0
        except OSError as error:
            raise ClientStorageError("failed to inspect bet storage") from error

    def _rollback_bet_storage(self, storage_size: int) -> None:
        try:
            os.truncate(self._server._lottery.storage_path, storage_size)
        except FileNotFoundError as error:
            if storage_size == 0:
                return
            raise ClientStorageError("failed to rollback bets") from error
        except OSError as error:
            raise ClientStorageError("failed to rollback bets") from error

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
