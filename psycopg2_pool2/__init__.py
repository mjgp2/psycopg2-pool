"""Enterprise connection pooling for psycopg2."""

import atexit
import contextlib
import ipaddress
import logging
import threading
import time
from collections import deque
from collections.abc import Generator
from dataclasses import dataclass
from random import random, shuffle
from socket import AF_INET, SOCK_STREAM, getaddrinfo
from time import monotonic
from typing import Self
from weakref import WeakKeyDictionary, WeakSet

import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_UNKNOWN, connection, parse_dsn

logger = logging.getLogger(__name__)

MESSAGE_WARNING_CLOSING_CONNECTION_WRONG_STATE = ("Closing checked out connection from pool because status is not IDLE (%s)")

MESSAGE_WARNING_ROLLBACK = "Transaction being returned in a non-IDLE status, rolling back (%s)"

MESSAGE_ERROR_SHUTDOWN = "Pool is shutdown"


class PoolError(Exception):
    """An error within the pool."""


def _is_ip_address(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    else:
        return True


@dataclass(frozen=True)
class PoolStats:
    """Pool connection statistics."""

    in_use: int
    idle: int


class ConnectionPool:
    """
    A pool of :class:`psycopg2:connection` objects.

    .. attribute:: minconn

        The minimum number of connections to keep warm in the pool. By default one
        connection is opened when the pool is created.

    .. attribute:: maxconn

        The maximum number of connections in the pool. By default the pool will
        attempt to open as many connections as requested.

    .. attribute:: idle_timeout

        How many seconds to keep an idle connection before closing it, approximately.

        Default 300.

    .. attribute:: lifetime_timeout

        How many seconds to use a connection for before closing it, approximately.

        Default 86400.

        Recommended to set to 300 for read replicas with DNS load balancing for even
        load following scale-up.

    .. attribute:: reap_idle_interval

        How often in seconds to reap the idle connections. Set to zero to disable.

        Default 300.

    .. attribute:: test_on_borrow

        If turned on, we will issue a `SELECT 1` to check the connection on borrow.

        Default on.

    .. attribute:: background_prewarm

        If turned on, do not block use until minconn connections are reached.

        Default on.

    .. attribute:: connect_kwargs

        The keyword arguments to pass to :func:`psycopg2.connect`. If the `dsn`
        argument isn't specified, then it's set to an empty string by default.

        If `connect_timeout` is not set, it will be set at 5 seconds to stop the pool
        locking up if there is an issue with DNS propogation for a host.

    The following attributes are internal, they're documented here to provide
    insight into how the pool works.

    .. attribute:: connections_in_use

        The set of connections that have been checked out of the pool through
        :meth:`getconn`. Type: :class:`weakref.WeakSet`.

    .. attribute:: idle_connections

        The pool of unused connections, last in first out.
        Type: :class:`collections.deque`.

    This class provides two main methods (:meth:`getconn` and :meth:`putconn`),
    plus another one that you probably don't need (:meth:`clear`).

    """

    __slots__ = (
        "minconn",
        "maxconn",
        "idle_timeout",
        "connect_kwargs",
        "connection_queue",
        "connections_idle",
        "connections_in_use",
        "expiry_times",
        "lifetime_timeout",
        "reaper_job",
        "__dict__",
        "lock",
        "test_on_borrow",
        "hostname",
        "reap_idle_interval",
        "prewarmed",
        "_shutdown",
    )

    def __init__(  # noqa: PLR0913
        self: Self,
        minconn: int = 1,
        maxconn: float = float("inf"),
        idle_timeout: int = 300,
        reap_idle_interval: float = 300,
        lifetime_timeout: int = 86400,
        *,
        test_on_borrow: bool = True,
        background_prewarm: bool = True,
        **connect_kwargs,  # noqa: ANN003
    ) -> None:
        """Initialize a pool."""
        self._shutdown = False
        self.minconn: int = minconn
        self.maxconn: float = maxconn
        self.idle_timeout: int = idle_timeout
        self.lifetime_timeout: int = lifetime_timeout
        self.test_on_borrow: bool = test_on_borrow
        connect_kwargs.setdefault("dsn", "")
        connect_kwargs.setdefault("connect_timeout", 5)
        self.connect_kwargs = connect_kwargs
        self.lock: threading.RLock = threading.RLock()

        parsed_dsn = parse_dsn(connect_kwargs["dsn"]) | connect_kwargs
        host = parsed_dsn["host"] if "host" in parsed_dsn and "hostaddr" not in parsed_dsn else None

        self.hostname = str(host) if host and not _is_ip_address(host) else None

        self.connections_in_use: WeakSet[connection] = WeakSet()
        self.connections_idle: WeakSet[connection] = WeakSet()

        self.connection_queue: deque[tuple[connection, float]] = deque()
        self.expiry_times: WeakKeyDictionary[connection, float] = WeakKeyDictionary()
        self.prewarmed = False
        if self.minconn:
            if background_prewarm:
                thread = threading.Thread(target=self._prewarm_loop, args=[60])
                thread.daemon = True
                thread.start()
            else:
                self._prewarm()

        if reap_idle_interval > 0:
            self.reaper_job = threading.Thread(target=self._reap_loop, args=[reap_idle_interval])
            self.reaper_job.daemon = True
            self.reaper_job.start()

        _pools.append(self)

    def _reap_loop(self: Self, interval: int) -> None:
        while not self._shutdown:
            try:
                self.reap_idle_connections()
            except Exception:
                logging.exception("Unexpected exception from reap_idle_connections")
            time.sleep(interval)

    def _prewarm_loop(self: Self, interval: int) -> None:
        while not self._shutdown:
            try:
                self._prewarm()
            except Exception:
                logging.exception("Unexpected exception from _prewarm")
            time.sleep(interval)

    def _prewarm(self: Self) -> None:
        try:
            for _ in range(self.minconn):
                stats = self.stats()
                if self._shutdown or stats.idle + stats.in_use >= self.minconn:
                    break
                self.putconn(self._getconn(checkout_connection=False))
        except Exception:
            logger.exception("Error prewarming pool")
        finally:
            self.prewarmed = True

    def shutdown(self: Self) -> None:
        """Shutdown this pool, stopping any spawned threads."""
        try:
            with self.lock:
                self._shutdown = True
            self.discard_all_idle()
        finally:
            _pools.remove(self)

    def _get_shuffled_hostaddr(self: Self) -> deque[str] | None:
        """Resolve DNS and return a list of IP addresses."""
        if not self.hostname:
            return None
        ips = [ip[4][0] for ip in getaddrinfo(self.hostname, 0, AF_INET, SOCK_STREAM)]
        shuffle(ips)
        logger.debug("Resovled ip addresses for hostname", extra={"ips": ips})
        return deque(ips)

    def _connect(self: Self, extra_args: dict | None) -> connection:
        """Open a new connection."""

        args = self.connect_kwargs | extra_args if extra_args else self.connect_kwargs

        logger.debug("Opening connection", extra={"connect_kwargs": args})

        conn = psycopg2.connect(**args)
        try:
            now = monotonic()
            with self.lock:
                self.expiry_times[conn] = (now + self.lifetime_timeout * (0.9 + 0.2 * random()))  # noqa: S311
                self.connections_in_use.add(conn)
        except Exception:
            conn.close()
            raise
        else:
            return conn

    def stats(self: Self) -> PoolStats:
        """Return statistics on pool usage."""
        with self.lock:
            return PoolStats(len(self.connections_in_use), len(self.connections_idle))

    def _checkin_connection(self: Self, conn: connection) -> None:
        now = monotonic()
        with self.lock:
            if conn in self.connections_idle:
                msg = "Connection already idle in pool"
                raise PoolError(msg)
            self.connections_idle.add(conn)
            # add to the back (right)
            self.connection_queue.append((conn, now))

    def _checkout_connection(self: Self) -> connection | None:
        try:
            with self.lock:
                # take from the front (left)
                conn, _ = self.connection_queue.popleft()
                self.connections_idle.remove(conn)
                self.connections_in_use.add(conn)
                return conn
        except IndexError:
            return None

    def _reap_connection_idle_too_long(self: Self) -> bool:
        try:
            with self.lock:
                if not self.connection_queue:
                    return False
                min_return_time = monotonic() - self.idle_timeout
                # the "longest idle" connection is at the front
                conn, return_time = self.connection_queue[0]
                if return_time > min_return_time:
                    return False

                # take from the front (left)
                idle_connection = self._checkout_connection()

                if idle_connection is None:
                    logger.warning("This should NEVER occur: null connection popped")
                    return False

                if idle_connection is not conn:
                    logger.warning("This should NEVER occur: incorrect connection popped, closing anyway")

                self._evict_connection(idle_connection)
                logger.info("Reaped connection idle too long, now %s connections", len(self.connection_queue))
                return True
        except IndexError:
            logger.warning("This should NEVER occur: index error")
            return False

    def _evict_connection(self: Self, conn: connection) -> None:
        with self.lock:
            if conn in self.connections_idle:
                msg = "Cannot evict idle connection"
                raise PoolError(msg)
            with contextlib.suppress(KeyError):
                self.connections_in_use.remove(conn)
        self._safely_close_connection(conn)

    def _safely_close_connection(self: Self, conn: connection) -> None:
        with contextlib.suppress(Exception):
            conn.close()

    @contextlib.contextmanager
    def conn(self: Self) -> Generator[connection, None, None]:
        """Yield a connection, and return it when the context manager completes."""

        conn = self._getconn()
        try:
            yield conn
        finally:
            self.putconn(conn)

    def getconn(self: Self) -> connection:
        """
        Get a tested connection from the pool.

        If there is no idle connection available, then a new one is opened;
        unless there are already :attr:`.maxconn` connections open, then a
        :class:`PoolError` exception is raised.
        """
        return self._getconn()

    def _getconn(self: Self, *, checkout_connection: bool = True) -> connection:

        if self._shutdown:
            raise PoolError(MESSAGE_ERROR_SHUTDOWN)

        ips: deque[str] | None = None
        # even if all the connections are broken in the pool, the max number of idle connections
        # in the deque would be minconn
        max_dequeues = max(self.minconn, 1) + 1
        dequeues = 0
        while dequeues < max_dequeues:
            conn = self._checkout_connection() if checkout_connection else None
            if conn is None:
                # We don't have any idle connection available, open a new one.
                if len(self.connections_in_use) >= self.maxconn:
                    msg = "connection pool exhausted"
                    raise PoolError(msg)

                # lazy resolve the IPs that we will try against
                if ips is None:
                    ips = self._get_shuffled_hostaddr()

                new_conn = self._attempt_connection(ips)

                if new_conn:
                    return new_conn

                continue

            dequeues += 1
            # validate connection is in a good state
            if self.test_on_borrow:
                self._do_select_1(conn)
            status = conn.info.transaction_status
            if status == TRANSACTION_STATUS_IDLE:
                return conn
            # connection in an invalid state
            logger.warning(MESSAGE_WARNING_CLOSING_CONNECTION_WRONG_STATE, status)
            self._evict_connection(conn)

        msg = f"Could not acquire a connection after {max_dequeues} tries"
        raise PoolError(msg)

    def _attempt_connection(self: Self, ips: deque[str] | None) -> connection | None:
        ip = ips.pop() if ips else None
        try:
            return self._connect(extra_args={"hostaddr": ip} if ip else None)
        except Exception:
            logger.warning("Failed to connect (hostaddr=%s)", ip)
            # If we've run out of IP addresses to try, raise the exception
            if not ips:
                raise
            return None

    def _do_select_1(self: Self, conn: connection) -> None:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.rollback()
        except Exception:
            # conn.info.transaction_status should now be UNKNOWN
            logger.exception("Connection failed test-on borrow")

    def putconn(self: Self, conn: connection) -> None:
        """
        Return a connection to the pool.

        You should always return a connection to the pool, even if you've closed
        it. That being said, the pool only holds weak references to connections
        returned by :meth:`getconn`, so they should be garbage collected even if
        you fail to return them.
        """
        with self.lock:
            if conn in self.connections_idle:
                msg = "Connection already idle in pool"
                raise PoolError(msg)
            if conn not in self.connections_in_use:
                msg = "Connection not in use in pool"
                raise PoolError(msg)
            self.connections_in_use.discard(conn)

        # cancel the current command, if one is running
        # this is a no-op if nothing is running
        if not conn.closed:
            with contextlib.suppress(Exception):
                conn.cancel()

        # Determine if the connection should be kept or discarded.
        if self._shutdown or (self.idle_timeout == 0 and len(self.connection_queue) >= self.minconn):
            conn.close()
        else:
            status = conn.info.transaction_status
            if status == TRANSACTION_STATUS_UNKNOWN:
                logger.warning("Transaction being returned in an UNKNOWN status")
                # The connection is broken, discard it.
                self._safely_close_connection(conn)
            elif not self._close_if_expired(conn):
                try:
                    if status != TRANSACTION_STATUS_IDLE:
                        logger.warning(MESSAGE_WARNING_ROLLBACK, status)
                        # The connection is still in a transaction, roll it back.
                        conn.rollback()
                    self._checkin_connection(conn)
                except Exception:  # noqa: BLE001
                    #  rollback failed, discard it
                    logger.info("Rollback of connection failed, closing it.")
                    self._safely_close_connection(conn)

    def reap_idle_connections(self: Self) -> None:
        """Close and discard idle connections in the pool that breached their idle timeout."""
        if self.idle_timeout == 0:
            logger.warning("reap_idle_connections should not be called if idle_timeout is zero")
            return

        q_len = len(self.connection_queue)
        logger.info("Cleaning up idle connections from %s connections in pool", q_len)
        close_count = 0
        for _ in range(q_len):
            if not self._reap_connection_idle_too_long():
                # nothing in the pool
                break
            close_count += 1

        logger.info("Closed %s connections", close_count)

        # create any connections necessary
        self._prewarm()

    def _close_if_expired(self: Self, conn: connection) -> bool:
        try:
            expiry = self.expiry_times[conn]
            if expiry < monotonic():
                logger.info("Closing connection as reached it's maximum lifetime")
                self._safely_close_connection(conn)
                del self.expiry_times[conn]
                return True
        except KeyError:
            logger.warning("Connection not in expiry map closing it")
            self._safely_close_connection(conn)
        except Exception:
            logger.exception("Couldn't close connection")
        return False

    def discard_all_idle(self: Self) -> None:
        """
        Close and discard all idle connections in the pool.

        This method could be useful if you have periods of high activity that
        result in many connections being opened, followed by prolonged periods
        with zero activity (no calls to :meth:`getconn` or :meth:`putconn`),
        *and* you care about closing those extraneous connections during the
        inactivity period. It's up to you to call this method in that case.

        This may cause more connections to be opened as we lock when getting
        list of connections to close, and then unlock while the connections are
        iterated through and closed.

        Alternatively you may want to run a cron task to `close idle connections
        from the server <https://stackoverflow.com/a/30769511/>`_.
        """
        # need a total lock, no new connections should be able to be made
        with self.lock:
            connections = list(self.connection_queue)
            self.connection_queue.clear()
            self.connections_idle.clear()
        for conn, _ in connections:
            self._safely_close_connection(conn)


# just an alias
ThreadSafeConnectionPool = ConnectionPool

_pools: list[ConnectionPool] = []


def _shutdown() -> None:
    for pool in _pools:
        pool.shutdown()


atexit.register(_shutdown)
