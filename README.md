
# psycopg2_pool2

Enterprise pooling of psycopg2 connections.

## Installation

You can install `psycopg2_pool2` using pip:

```bash
pip install psycopg2_pool2
```

## Usage

Here is an example of how to use `psycopg2_pool2`:

```python
from psycopg2_pool2 import ConnectionPool

# Create a connection pool
pool = ConnectionPool(
    minconn=1,
    maxconn=10,
    idle_timeout=300,
    reap_idle_interval=300,
    lifetime_timeout=86400,
    test_on_borrow=True,
    background_prewarm=True,
    dsn="dbname=postgres user=postgres" # use PGPASSWORD env variable
)

# Get a connection from the pool
with pool.conn() as conn, conn.cursor() as cur:
    cur.execute("SELECT 1")
    print(cur.fetchone())
    conn.rollback()
```

## Features

### Pool connections

Specify a target minimum and maximum number of connections. A `PoolError` will be raised if we already have `maxconn` connections checked out.

Connections are used in a FIFO order.

### Non-blockiong startup

By default the `minconn` connections are `prewarmed` in a separate thread, and this thread then will monitor that we still have the correct number of warm connections in the pool.

### Connection expiry

Connections can be expired by setting their approximate maximum lifetime, `lifetime_timeout`. This is given a ±10% jitter.

For DNS-based database load balancing (e.g. RDS Aurora read replicas), it is necessary to give connections a limited lifespan to allow connections to reach new replicas if connection pool is stable. For this use case 5-10 minutes is a reasonable lifespan.

Furthermore it is considered best practice for Postgres to close connections after a day as there can be memory fragmentation, etc. Hence the default expiry is one day.

### Idle connection reaper thread

Reaping can be done in a separate thread automatically on a configurable interval by setting the `reap_idle_interval` to a positive value.

External services can also call the `reap_idle_connections` method if this tasks is externally managed.

### Reduced thread lock contention

Thread locks are used to ensure thread-safety, but locks are held for a minimal amount of time, rather than being held for the entire method call.

### Throughput optimization

The `getconn` and `putconn` methods are optimized such that their return is not delayed by either reaping idle connections or opening up (possibly multiple) new connections to meet the desired minimum connection count.

This creates a known expected upper bound for these calls.

### Test-on-borrow

Connections are by default validated before being returned from putconn by issuing a `SELECT 1`.

This can be disabled with the `test_on_borrow` flag.

Note that it is possible for the number of connections to fall below the mininum number if they are closed due to a test-on-borrow failure, but every minute we will "prewarm" if `background_prewarm` is true.

### Statistics

The `stats` method to return stats on in use and idle connections.

### Host address randomization and fallback

If a `host` is specified in the connection args or within the `dsn` that is a hostname (i.e. not an IP address) when attempting to connect to the host we will try each IP in a random order.

This allows us to handle cases where a read-replica is down or overloaded, and connect to another, as well as balancing the load more effectively over the set of replicas pointed to by the hostname.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the LGPL3 License. See the `LICENSE` file for details.

The project was originally forked from: https://github.com/Changaco/psycopg2-pool/