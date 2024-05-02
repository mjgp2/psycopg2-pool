"""This module implements connection pooling, which is needed because PostgreSQL
requires separate TCP connections for concurrent sessions.
"""

import contextlib
from dataclasses import dataclass
import logging
from socket import AF_INET, SOCK_STREAM, getaddrinfo
import threading
from collections import deque
from random import random, shuffle
from time import monotonic
from typing import Self
from weakref import WeakKeyDictionary, WeakSet
import ipaddress

import psycopg2
from psycopg2.extensions import connection, TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_UNKNOWN

logger = logging.getLogger(__name__)

MESSAGE_WARNING_CLOSING_CONNECTION_WRONG_STATE = (
    "Closing checked out connection from pool because status is not IDLE (%s)"
)

MESSAGE_WARNING_ROLLBACK = "Transaction being returned in a non-IDLE status, rolling back (%s)"


class PoolError(Exception):
    pass


def is_ip_address(host) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


@dataclass(frozen=True)
class PoolStats:
    in_use: int
    idle: int


class ConnectionPool:
    """A pool of :class:`psycopg2:connection` objects.

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
        'minconn', 'maxconn', 'idle_timeout', 'connect_kwargs',
        'connection_queue', 'connections_idle', 'connections_in_use',
        'expiry_times', 'lifetime_timeout', 'reaper_job',
        '__dict__', 'lock', 'test_on_borrow', 'hostname', 'reap_idle_interval',
        'prewarmed',
    )

    def __init__(
                self: Self,
                minconn: int = 1,
                maxconn: float = float('inf'),
                idle_timeout: int = 300,
                reap_idle_interval: float = 300,
                lifetime_timeout: int = 86400,
                test_on_borrow: bool = True,
                background_prewarm: bool = True,
                **connect_kwargs
            ) -> None:
        self.minconn: int = minconn
        self.maxconn: float = maxconn
        self.idle_timeout: int = idle_timeout
        self.lifetime_timeout: int = lifetime_timeout
        if reap_idle_interval > 0:
            self.reaper_job = threading.Timer(
                reap_idle_interval,
                self.reap_idle_connections,
            )
            self.reaper_job.daemon = True
            self.reaper_job.start()
        self.test_on_borrow: bool = test_on_borrow
        connect_kwargs.setdefault('dsn', '')
        connect_kwargs.setdefault('connect_timeout', 5)
        self.connect_kwargs = connect_kwargs
        self.lock: threading.RLock = threading.RLock()
        self.hostname = (
                str(connect_kwargs['host'])
                if 'host' in connect_kwargs and not is_ip_address(connect_kwargs['host'])
                else None
            )
        self.connections_in_use: WeakSet[connection] = WeakSet()
        self.connections_idle: WeakSet[connection] = WeakSet()

        self.connection_queue: deque[tuple[connection, float]] = deque()
        self.expiry_times: WeakKeyDictionary[connection, float] = WeakKeyDictionary()
        self.prewarmed = False
        if self.minconn:
            if background_prewarm:
                thread = threading.Thread(target=self.prewarm)
                thread.daemon = True
                thread.start()
            else:
                self.prewarm()

    def prewarm(self: Self) -> None:
        try:
            for _ in range(0, self.minconn):
                stats = self.stats()
                if stats.idle + stats.in_use >= self.minconn:
                    break
                self.putconn(self._getconn(False))
        except Exception:
            logger.exception("Error prewarming pool")
        finally:
            self.prewarmed = True

    def shutdown(self: Self) -> None:
        self.reaper_job.cancel()

    def get_shuffled_hostaddr(self: Self) -> list[str] | None:
        """Resolve DNS and return a list of IP addresses."""
        if not self.hostname:
            return None
        ips = [ip[4][0] for ip in getaddrinfo(self.hostname, 0, AF_INET, SOCK_STREAM)]
        shuffle(ips)
        return ips

    def _connect(self: Self, extra_args: dict | None) -> connection:
        """Open a new connection.
        """
        if extra_args:
            args = self.connect_kwargs | extra_args
        else:
            args = self.connect_kwargs

        conn = psycopg2.connect(**args)
        try:
            now = monotonic()
            with self.lock:
                self.expiry_times[conn] = (
                        now +
                        self.lifetime_timeout * (0.9 + 0.2 * random())
                    )
                self.connections_in_use.add(conn)
            return conn
        except Exception:
            conn.close()
            raise

    def stats(self: Self) -> PoolStats:
        with self.lock:
            return PoolStats(len(self.connections_in_use), len(self.connections_idle))

    def _checkin_connection(self, conn) -> None:
        now = monotonic()
        with self.lock:
            if conn in self.connections_idle:
                raise PoolError("Connection already idle in pool")
            self.connections_idle.add(conn)
            self.connection_queue.append((conn, now))

    def _checkout_connection(self) -> connection | None:
        try:
            with self.lock:
                conn, _ = self.connection_queue.pop()
                self.connections_idle.remove(conn)
                self.connections_in_use.add(conn)
                return conn
        except IndexError:
            return None

    def _reap_connection_idle_too_long(self) -> bool:
        try:
            with self.lock:
                # the "longest idle" connection
                conn, return_time = self.connection_queue[0]
                if return_time < (monotonic() - self.idle_timeout):
                    self._safely_close_connection(conn)
                    popped, return_time = self.connection_queue.pop()
                    if popped is not conn:
                        logger.warning("This should NEVER occur: incorrect connection popped")
                        self.connection_queue.appendleft((popped, return_time))
                        return False
                    logger.info("Reaped connection idle too long")
                    return True
        except IndexError:
            pass
        return False

    def _evict_connection(self: Self, conn: connection) -> None:
        with self.lock:
            if conn in self.connections_idle:
                raise PoolError("Cannot evict idle connection")
            with contextlib.suppress(KeyError):
                self.connections_in_use.remove(conn)
        self._safely_close_connection(conn)

    def _safely_close_connection(self: Self, conn: connection) -> None:
        with contextlib.suppress(Exception):
            conn.close()

    def getconn(self: Self) -> connection:
        """Get a connection from the pool.

        If there is no idle connection available, then a new one is opened;
        unless there are already :attr:`.maxconn` connections open, then a
        :class:`PoolError` exception is raised.

        Any connection that is broken, or has been idle for more than
        :attr:`.idle_timeout` seconds, is closed and discarded.
        """

        return self._getconn()

    def _getconn(self: Self, checkout_connection: bool = True) -> connection:
        ips: list[str] | None = None
        # even if all the connections are broken in the pool, the max number of idle connections
        # in the deque would be minconn
        max_dequeues = max(self.minconn, 1) + 1
        dequeues = 0
        while dequeues < max_dequeues:
            conn = self._checkout_connection() if checkout_connection else None
            if conn is None:
                # We don't have any idle connection available, open a new one.
                if len(self.connections_in_use) >= self.maxconn:
                    raise PoolError("connection pool exhausted")

                # lazy resolve the IPs that we will try against
                if ips is None:
                    ips = self.get_shuffled_hostaddr()

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

        raise PoolError(f"Could not acquire a connection after {self.minconn} tries")

    def _attempt_connection(self, ips) -> connection | None:
        ip = ips.pop() if ips else None
        try:
            return self._connect(extra_args={"hostaddr": ip} if ip else None)
        except Exception:
            logger.warning("Failed to connect (hostaddr=%s)", ip)
            # If we've run out of IP addresses to try, raise the exception
            if not ips:
                raise

    def _do_select_1(self: Self, conn: connection):
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.rollback()
        except Exception:
            # conn.info.transaction_status should now be UNKNOWN
            logger.exception("Connection failed test-on borrow")

    def putconn(self: Self, conn: connection) -> None:
        """Return a connection to the pool.

        You should always return a connection to the pool, even if you've closed
        it. That being said, the pool only holds weak references to connections
        returned by :meth:`getconn`, so they should be garbage collected even if
        you fail to return them.
        """
        with self.lock:
            if conn in self.connections_idle:
                raise PoolError("Connection already idle in pool")
            if conn not in self.connections_in_use:
                raise PoolError("Connection not in use in pool")
            self.connections_in_use.discard(conn)

        # cancel the current command, if one is running
        # this is a no-op if nothing is running
        conn.cancel()

        # Determine if the connection should be kept or discarded.
        # close_surplus_connections_immediately = self.idle_timeout == 0
        if self.idle_timeout == 0 and len(self.connection_queue) >= self.minconn:
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
                except Exception:
                    #  rollback failed, discard it
                    logger.info("Rollback of connection failed, closing it.")
                    self._safely_close_connection(conn)

    def reap_idle_connections(self: Self) -> None:
        """Close and discard idle connections in the pool that breached their
        idle timeout.
        """
        if self.idle_timeout == 0:
            logger.warning("reap_idle_connections should not be called if idle_timeout is zero")
            return

        logger.warning("Cleaning up idle connections")
        for _ in range(len(self.connection_queue)):
            if not self._reap_connection_idle_too_long():
                # nothing in the pool
                break

    def _close_if_expired(self: Self, conn: connection) -> bool:
        try:
            expiry = self.expiry_times[conn]
            if expiry < monotonic():
                logger.info("Closing connection as reached it's maximum lifetime")
                self._safely_close_connection(conn)
                del self.expiry_times[conn]
                return True
        except KeyError:
            logging.warning("Connection not in expiry map closing it")
            self._safely_close_connection(conn)
        except Exception:
            logging.exception("Couldn't close connection")
        return False

    def clear(self: Self):
        """Close and discard all idle connections in the pool (regardless of the
        values of :attr:`.minconn` and :attr:`.idle_timeout`).

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
