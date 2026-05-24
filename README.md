# InMemoryEmulator.Redis

[![NuGet](https://img.shields.io/nuget/v/InMemoryEmulator.Redis.svg)](https://www.nuget.org/packages/InMemoryEmulator.Redis)
[![CI](https://github.com/lemonlion/InMemoryEmulator.Redis/actions/workflows/test.yml/badge.svg)](https://github.com/lemonlion/InMemoryEmulator.Redis/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A high-fidelity, in-process fake for the StackExchange.Redis .NET client — purpose-built for fast, reliable integration testing. Zero Docker, instant startup, full SDK fidelity.

## Quick Start

### Install

```bash
dotnet add package InMemoryEmulator.Redis
```

### Direct Instantiation

```csharp
// Create an in-memory Redis instance — one liner
await using var redis = await InMemoryRedis.CreateAsync();
var db = redis.Database;

// Use the real StackExchange.Redis IDatabase — all calls go through the fake RESP server
await db.StringSetAsync("user:1:name", "Alice");
await db.HashSetAsync("user:1", new HashEntry[] { new("age", "30"), new("city", "London") });
await db.SortedSetAddAsync("leaderboard", new SortedSetEntry[] { new("Alice", 100), new("Bob", 85) });

var name = await db.StringGetAsync("user:1:name"); // "Alice"
var topPlayers = await db.SortedSetRangeByRankAsync("leaderboard", 0, -1, Order.Descending);
```

### DI Integration

For ASP.NET Core integration testing with `WebApplicationFactory`:

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryRedis(options =>
    {
        options.OnCreated = result =>
        {
            // Seed test data
            result.Database.StringSet("config:timeout", "30");
        };
    });
});
```

## How It Works

The emulator runs an in-process TCP server that speaks the Redis RESP protocol. A real `ConnectionMultiplexer` connects to it exactly as it would connect to a real Redis server — the SDK doesn't know the difference.

```
Your Code → ConnectionMultiplexer (real SDK) → TCP/RESP → FakeRedisServer (in-process) → InMemoryRedisStore
```

This means:

- **Full SDK fidelity** — multiplexing, pipelining, RESP serialization, retry logic all work exactly as in production
- **Zero production code changes** — no special interfaces, no mocking, no conditional logic
- **Real `IConnectionMultiplexer` / `IDatabase`** — your tests use the exact same types as production code

## Features

### Supported

- **Strings** — GET, SET (NX/XX/EX/PX/EXAT/PXAT/KEEPTTL/GET), MGET, MSET, MSETNX, INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY, APPEND, STRLEN, GETRANGE, SETRANGE, GETDEL, GETEX, SETNX, SETEX, PSETEX
- **Hashes** — HSET, HGET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN, HMSET, HMGET, HINCRBY, HINCRBYFLOAT, HSETNX, HRANDFIELD, HSCAN
- **Lists** — LPUSH, RPUSH, LPUSHX, RPUSHX, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, RPOPLPUSH, LMOVE, LPOS
- **Sets** — SADD, SREM, SMEMBERS, SISMEMBER, SMISMEMBER, SCARD, SPOP, SRANDMEMBER, SINTER, SINTERCARD, SINTERSTORE, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE, SMOVE, SSCAN
- **Sorted Sets** — ZADD (NX/XX/GT/LT/CH), ZREM, ZSCORE, ZMSCORE, ZRANK, ZREVRANK, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZLEXCOUNT, ZPOPMIN, ZPOPMAX, ZRANDMEMBER, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE, ZSCAN
- **Keys** — DEL, UNLINK, EXISTS, EXPIRE (NX/XX/GT/LT), PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, TTL, PTTL, EXPIRETIME, PEXPIRETIME, TYPE, RENAME, RENAMENX, KEYS, SCAN, RANDOMKEY, COPY, TOUCH, OBJECT
- **Pub/Sub** — SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB
- **Transactions** — MULTI, EXEC, DISCARD, WATCH, UNWATCH
- **Streams** — XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM, XDEL, XINFO, XGROUP, XREADGROUP, XACK, XPENDING
- **HyperLogLog** — PFADD, PFCOUNT, PFMERGE
- **Geo** — GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH
- **Scripting** — EVAL, EVALSHA, SCRIPT (LOAD/EXISTS/FLUSH)
- **Server** — PING, ECHO, INFO, SELECT, DBSIZE, FLUSHDB, FLUSHALL, TIME, CONFIG, CLIENT, COMMAND
- **Key expiration** — lazy + active expiry matching real Redis behavior
- **Fault injection** — simulate errors for any command
- **Command logging** — record all operations for test assertions
- **State persistence** — export/import store as JSON
- **DI integration** — `UseInMemoryRedis()` for `IServiceCollection`

### Not Supported

- Cluster mode (standalone only)
- Replication (SLAVEOF, REPLICAOF)
- ACL system (simple password AUTH only)
- Persistence (RDB/AOF)
- Memory limits / eviction policies
- Redis Modules (RedisSearch, RedisJSON, etc.)
- Blocking commands (BLPOP, BRPOP — planned)

## Builder Pattern

For advanced configuration:

```csharp
await using var redis = await InMemoryRedis.Builder()
    .WithPassword("secret")
    .WithSeedData(store => { /* pre-populate */ })
    .WithFaultInjector((cmd, args) =>
    {
        if (cmd == "GET") return "simulated failure";
        return null; // pass through
    })
    .ConfigureConnection(opts => opts.ConnectTimeout = 10000)
    .BuildAsync();
```

## Three-Tier Access

```csharp
var redis = await InMemoryRedis.CreateAsync();

// Tier 1: Production-like SDK access
IConnectionMultiplexer mux = redis.Multiplexer;
IDatabase db = redis.Database;

// Tier 2: Direct store access (test setup)
redis.Store.FlushAll();
redis.Store.ImportState(json);

// Tier 3: Server-level diagnostics
redis.Server.FaultInjector = (cmd, args) => /* ... */;
redis.CommandLog.GetByCommand("SET");
```

## Comparison with Docker Redis

| | InMemoryEmulator.Redis | Docker Redis |
|-|----------------------|--------------|
| **Startup time** | Instant (<10ms) | 2-5 seconds |
| **Dependencies** | None (NuGet only) | Docker Desktop |
| **CI suitability** | Excellent | Requires Docker service |
| **Fault injection** | Built-in | Not available |
| **Command logging** | Built-in | Requires MONITOR |
| **Thread safety** | Full | N/A (separate process) |
| **Feature coverage** | Core commands | All commands |

## Parity Testing

The test suite runs against both the in-memory emulator and real Redis 7 in Docker (via Testcontainers) to ensure identical behavior:

```bash
# Default: run against in-memory emulator
dotnet test

# Run against real Redis for parity verification
REDIS_TEST_TARGET=Docker dotnet test
```

192 integration tests verified against real Redis.

## Supported SDK Versions

- `StackExchange.Redis` >= 2.8.0
- .NET 8.0+

## Documentation

See the [wiki](https://github.com/lemonlion/InMemoryEmulator.Redis/wiki) for full documentation including:

- [Getting Started](https://github.com/lemonlion/InMemoryEmulator.Redis/wiki/Getting-Started)
- [How It Works](https://github.com/lemonlion/InMemoryEmulator.Redis/wiki/How-It-Works)
- [Setup Guide](https://github.com/lemonlion/InMemoryEmulator.Redis/wiki/Setup-Guide)
- [Features](https://github.com/lemonlion/InMemoryEmulator.Redis/wiki/Features)
- [API Reference](https://github.com/lemonlion/InMemoryEmulator.Redis/wiki/API-Reference)
- [Known Limitations](https://github.com/lemonlion/InMemoryEmulator.Redis/wiki/Known-Limitations)
- [Troubleshooting](https://github.com/lemonlion/InMemoryEmulator.Redis/wiki/Troubleshooting)

## License

[MIT](LICENSE)
