from __future__ import absolute_import, division, print_function, unicode_literals

from socket import gaierror
from time import monotonic, sleep
from unittest import TestCase

import psycopg2
from psycopg2.errors import ProgrammingError
import psycopg2.extensions as _ext

from psycopg2_pool import ConnectionPool, PoolError, PoolStats


class PoolTests(TestCase):

    def test_defaults(self):
        pool = ConnectionPool(0, 1, reap_idle_interval=0)
        assert pool.connect_kwargs['connect_timeout'] == 5
        assert pool.connect_kwargs['dsn'] == ''

    def test_getconn(self):
        pool = ConnectionPool(0, 1, reap_idle_interval=0)

        conn = pool.getconn()

        # Make sure we got an open connection
        assert not conn.closed

        assert pool.stats() == PoolStats(1, 0)

        # Try again. We should get an error, since we only allowed one connection.
        with self.assertRaises(PoolError):
            pool.getconn()

        # Put the connection back, the return time should be saved
        now = monotonic()
        pool.putconn(conn)
        assert len(pool.connection_queue) == 1
        assert conn is pool.connection_queue[0][0]
        assert now < pool.connection_queue[0][1]

        # Get the connection back
        new_conn = pool.getconn()
        assert new_conn is conn
        assert len(pool.connection_queue) == 0

    def test_putconn(self):
        pool = ConnectionPool(0, 1, reap_idle_interval=0)
        conn = pool.getconn()
        assert len(pool.connection_queue) == 0

        pool.putconn(conn)
        with self.assertRaises(PoolError):
            pool.putconn(conn)

    def test_putconn_twice(self):
        pool = ConnectionPool(0, 1, reap_idle_interval=0)
        conn = pool.getconn()
        assert len(pool.connection_queue) == 0

        now = monotonic()
        pool.putconn(conn)
        assert len(pool.connection_queue) == 1
        assert conn is pool.connection_queue[0][0]
        assert now < pool.connection_queue[0][1]

    def test_putconn_with_close_connection(self):
        pool = ConnectionPool(0, 1, idle_timeout=0, reap_idle_interval=0)
        conn = pool.getconn()
        assert len(pool.connection_queue) == 0
        assert conn in pool.connections_in_use

        conn.close()
        pool.putconn(conn)
        assert len(pool.connection_queue) == 0

    def test_putconn_with_expired_connection(self):
        pool = ConnectionPool(0, 1, idle_timeout=60, lifetime_timeout=0, reap_idle_interval=0)
        conn = pool.getconn()
        assert len(pool.connection_queue) == 0
        assert conn in pool.connections_in_use

        pool.putconn(conn)

        assert len(pool.connection_queue) == 0

    def test_getconn_closed_no_test_on_borrow(self):
        pool = ConnectionPool(0, 1, test_on_borrow=False, reap_idle_interval=0)
        conn = pool.getconn()
        pool.putconn(conn)

        # Close the connection, it should still be in the pool
        conn.close()

        # The connection should be discarded by getconn
        new_conn = pool.getconn()
        assert new_conn is not conn

    def test_getconn_closed(self):
        pool = ConnectionPool(0, 1, reap_idle_interval=0)
        conn = pool.getconn()
        now = monotonic()
        pool.putconn(conn)

        # Close the connection, it should still be in the pool
        conn.close()
        assert len(pool.connection_queue) == 1
        assert conn is pool.connection_queue[0][0]
        assert now < pool.connection_queue[0][1]

        # The connection should be discarded by getconn
        new_conn = pool.getconn()
        assert new_conn is not conn

    def test_reap_idle_connections(self):
        pool = ConnectionPool(0, 1, idle_timeout=30, reap_idle_interval=0)
        conn = pool.getconn()

        # Expire the connection
        pool.putconn(conn)

        assert len(pool.connection_queue) == 1
        assert conn is pool.connection_queue[0][0]

        pool.connection_queue[0] = (pool.connection_queue[0][0], pool.connection_queue[0][1] - 60)

        pool.reap_idle_connections()

        # Connection should be discarded
        new_conn = pool.getconn()
        assert new_conn is not conn

        # simulate race condition
        assert len(pool.connection_queue) == 0
        assert not pool._reap_connection_idle_too_long()

    def test_reap_idle_connections_auto(self):
        pool = ConnectionPool(0, 1, idle_timeout=30, reap_idle_interval=0.1)
        conn = pool.getconn()

        # Expire the connection
        pool.putconn(conn)

        assert len(pool.connection_queue) == 1
        assert conn is pool.connection_queue[0][0]

        pool.connection_queue[0] = (pool.connection_queue[0][0], pool.connection_queue[0][1] - 60)

        sleep(0.2)

        # Connection should be discarded
        new_conn = pool.getconn()
        assert new_conn is not conn

        pool.shutdown()

    def test_putconn_errorState(self):
        pool = ConnectionPool(0, 1, reap_idle_interval=0)
        conn = pool.getconn()

        # Get connection into transaction state
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO nonexistent (id) VALUES (1)")
        except ProgrammingError:
            pass

        assert conn.get_transaction_status() != _ext.TRANSACTION_STATUS_IDLE

        pool.putconn(conn)

        # Make sure we got back into the pool and are now showing idle
        assert conn.get_transaction_status() == _ext.TRANSACTION_STATUS_IDLE
        assert conn is pool.connection_queue[0][0]

    def test_putconn_closed(self):
        pool = ConnectionPool(0, 1, reap_idle_interval=0)
        conn = pool.getconn()

        # The connection should be open and shouldn't have a return time
        assert not conn.closed

        conn.close()

        # Now should be closed
        assert conn.closed

        pool.putconn(conn)

        assert len(pool.connection_queue) == 0

    def test_caching(self):
        pool = ConnectionPool(0, 10, reap_idle_interval=0)

        # Get a connection to use to check the number of connections
        check_conn = pool.getconn()
        check_conn.autocommit = True  # Being in a transaction hides the other connections.
        # Get a cursor to run check queries with
        check_cursor = check_conn.cursor()

        SQL = """
            SELECT numbackends
              FROM pg_stat_database
             WHERE datname = current_database()
        """
        check_cursor.execute(SQL)

        # Not trying to test anything yet, so hopefully this always works :)
        starting_conns = check_cursor.fetchone()[0]  # type: ignore

        # Get a couple more connections
        conn2 = pool.getconn()
        conn3 = pool.getconn()

        assert conn2 != conn3
        assert pool.stats() == PoolStats(3, 0)

        # Verify that we have the expected number of connections to the DB server now
        check_cursor.execute(SQL)
        total_cons = check_cursor.fetchone()[0]  # type: ignore

        assert total_cons == starting_conns + 2

        # Put the connections back in the pool and verify they don't close
        pool.putconn(conn2)
        pool.putconn(conn3)

        check_cursor.execute(SQL)
        total_cons_after_put = check_cursor.fetchone()[0]  # type: ignore

        assert total_cons == total_cons_after_put

        # Get another connection and verify we don't create a new one
        conn4 = pool.getconn()

        # conn4 should be either conn2 or conn3 (we don't care which)
        assert conn4 in (conn2, conn3)

        check_cursor.execute(SQL)
        total_cons_after_get = check_cursor.fetchone()[0]  # type: ignore

        assert total_cons_after_get == total_cons

    def test_clear(self):
        pool = ConnectionPool(0, 10, reap_idle_interval=0)
        conn1 = pool.getconn()
        conn2 = pool.getconn()
        conn3 = pool.getconn()
        pool.putconn(conn3)

        assert len(pool.connection_queue) == 1
        assert pool.stats() == PoolStats(2, 1)
        assert not conn1.closed
        assert not conn2.closed
        assert not conn3.closed

        pool.clear()

        assert not conn1.closed
        assert not conn2.closed
        assert conn3.closed
        assert len(pool.connection_queue) == 0
        assert pool.stats() == PoolStats(2, 0)

    def test_close_if_expired_error(self):
        pool = ConnectionPool(0, 10, reap_idle_interval=0)
        assert not pool._close_if_expired(('not a connection',))  # type: ignore

    def test_close_if_expired_missing_from_map(self):
        pool = ConnectionPool(0, 10, reap_idle_interval=0)
        conn = psycopg2.connect()
        assert not pool._close_if_expired(conn)  # type: ignore
        assert conn.closed

    def test_host_random_resolution(self):
        pool = ConnectionPool(0, 1, host="localhost", reap_idle_interval=0)
        pool.getconn()

    def test_host_random_resolution_failure(self):
        pool = ConnectionPool(0, 1, host="jsdfgjhsdfkhsdk.foo", reap_idle_interval=0)
        # Try again. We should get an error, since we only allowed one connection.
        with self.assertRaises(gaierror):
            pool.getconn()

    def test_connect_timeout(self):
        pool = ConnectionPool(0, 1, host="bing.com", connect_timeout=0.2, reap_idle_interval=0)
        # Try again. We should get an error, since we only allowed one connection.
        with self.assertRaises(psycopg2.OperationalError):
            pool.getconn()

    def test_host_ip_address(self):
        pool = ConnectionPool(0, 1, host='127.0.0.1', reap_idle_interval=0)
        pool.getconn()

    def test_prewarm(self):
        pool = ConnectionPool(1, 1, background_prewarm=False, reap_idle_interval=0)
        sleep(2)
        assert pool.stats().idle == 1
        assert pool.prewarmed

    def test_prewarm_background(self):
        pool = ConnectionPool(5, 10, reap_idle_interval=0)
        sleep(3)
        assert pool.stats().idle == 5
        assert pool.prewarmed

