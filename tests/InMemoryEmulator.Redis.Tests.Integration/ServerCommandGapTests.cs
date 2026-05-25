using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ServerCommandGapTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;
    private IConnectionMultiplexer _mux = null!;

    public ServerCommandGapTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        _mux = await _fixture.GetMultiplexerAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ═══════════════════════════════════════════════════════════════
    // TIME
    // Ref: https://redis.io/docs/latest/commands/time/
    //   "The TIME command returns the current server time as a
    //    two items lists: a Unix timestamp and the amount of
    //    microseconds already elapsed in the current second."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task TIME_returns_seconds_and_microseconds()
    {
        var result = await _db.ExecuteAsync("TIME");
        Assert.False(result.IsNull);

        var arr = (RedisResult[])result!;
        Assert.Equal(2, arr.Length);

        // First element: Unix timestamp in seconds
        var seconds = long.Parse(arr[0].ToString()!);
        Assert.True(seconds > 0, "Unix timestamp should be positive");

        // Second element: microseconds (0 - 999999)
        var microseconds = long.Parse(arr[1].ToString()!);
        Assert.InRange(microseconds, 0, 999999);
    }

    // ═══════════════════════════════════════════════════════════════
    // CONFIG GET / CONFIG SET
    // Ref: https://redis.io/docs/latest/commands/config-get/
    //   "The CONFIG GET command is used to read the configuration
    //    parameters of a running Redis server."
    // Ref: https://redis.io/docs/latest/commands/config-set/
    //   "The CONFIG SET command is used in order to reconfigure
    //    the server at run time."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task CONFIG_GET_returns_parameter_value()
    {
        var result = await _db.ExecuteAsync("CONFIG", "GET", "timeout");
        Assert.False(result.IsNull);

        var arr = (RedisResult[])result!;
        // CONFIG GET returns pairs: [name, value, name, value, ...]
        // For a single parameter, should return 2 elements (or 0 if not found)
        Assert.True(arr.Length == 0 || arr.Length >= 2,
            $"CONFIG GET should return 0 or 2+ elements, got {arr.Length}");

        if (arr.Length >= 2)
        {
            Assert.Equal("timeout", arr[0].ToString());
        }
    }

    [Fact]
    public async Task CONFIG_SET_accepts_parameter()
    {
        // Ref: https://redis.io/docs/latest/commands/config-set/
        //   CONFIG SET returns OK on success

        var result = await _db.ExecuteAsync("CONFIG", "SET", "hz", "20");
        Assert.Equal("OK", result.ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // CLIENT ID
    // Ref: https://redis.io/docs/latest/commands/client-id/
    //   "The command just returns the ID of the current connection."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task CLIENT_ID_returns_integer()
    {
        var result = await _db.ExecuteAsync("CLIENT", "ID");
        Assert.False(result.IsNull);

        var clientId = (long)result;
        Assert.True(clientId >= 0, $"Client ID should be non-negative, got {clientId}");
    }

    // ═══════════════════════════════════════════════════════════════
    // CLIENT GETNAME / CLIENT SETNAME
    // Ref: https://redis.io/docs/latest/commands/client-setname/
    //   "The CLIENT SETNAME command assigns a name to the current connection."
    // Ref: https://redis.io/docs/latest/commands/client-getname/
    //   "The CLIENT GETNAME returns the name of the current connection
    //    as set by CLIENT SETNAME."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task CLIENT_SETNAME_and_GETNAME_roundtrip()
    {
        // Act: set the client name
        var setResult = await _db.ExecuteAsync("CLIENT", "SETNAME", "my-test-client");
        Assert.Equal("OK", setResult.ToString());

        // Act: get the client name
        var getResult = await _db.ExecuteAsync("CLIENT", "GETNAME");
        Assert.Equal("my-test-client", getResult.ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // CLIENT LIST
    // Ref: https://redis.io/docs/latest/commands/client-list/
    //   "The CLIENT LIST command returns information and statistics
    //    about the client connections server in a mostly human-readable format."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task CLIENT_LIST_returns_client_info_string()
    {
        var result = await _db.ExecuteAsync("CLIENT", "LIST");
        Assert.False(result.IsNull);

        var info = result.ToString();
        Assert.NotNull(info);
        Assert.True(info!.Length > 0, "CLIENT LIST should return non-empty string");
        // CLIENT LIST output contains id= field
        Assert.Contains("id=", info);
    }

    // ═══════════════════════════════════════════════════════════════
    // CLIENT INFO
    // Ref: https://redis.io/docs/latest/commands/client-info/
    //   "The command returns information and statistics about
    //    the current client connection in a mostly human-readable format."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task CLIENT_INFO_returns_current_client_info()
    {
        var result = await _db.ExecuteAsync("CLIENT", "INFO");
        Assert.False(result.IsNull);

        var info = result.ToString();
        Assert.NotNull(info);
        Assert.True(info!.Length > 0, "CLIENT INFO should return non-empty string");
        // Should contain id= field
        Assert.Contains("id=", info);
    }

    // ═══════════════════════════════════════════════════════════════
    // SLOWLOG GET / LEN / RESET
    // Ref: https://redis.io/docs/latest/commands/slowlog-get/
    //   "The Redis Slow Log is a system to log queries that exceeded
    //    a specified execution time."
    // Ref: https://redis.io/docs/latest/commands/slowlog-len/
    //   "Returns the number of entries in the slow log."
    // Ref: https://redis.io/docs/latest/commands/slowlog-reset/
    //   "Resets the slow log, clearing all entries in it."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task SLOWLOG_GET_returns_entries()
    {
        var result = await _db.ExecuteAsync("SLOWLOG", "GET");
        Assert.False(result.IsNull);

        // Result is an array (may be empty)
        var arr = (RedisResult[])result!;
        Assert.NotNull(arr);
        // Just verify it doesn't error — entries may be empty
    }

    [Fact]
    public async Task SLOWLOG_LEN_returns_count()
    {
        var result = await _db.ExecuteAsync("SLOWLOG", "LEN");
        Assert.False(result.IsNull);

        var len = (long)result;
        Assert.True(len >= 0, $"SLOWLOG LEN should be non-negative, got {len}");
    }

    [Fact]
    public async Task SLOWLOG_RESET_clears_entries()
    {
        var result = await _db.ExecuteAsync("SLOWLOG", "RESET");
        Assert.Equal("OK", result.ToString());

        // After reset, length should be 0
        var len = (long)await _db.ExecuteAsync("SLOWLOG", "LEN");
        Assert.Equal(0, len);
    }

    // ═══════════════════════════════════════════════════════════════
    // MEMORY USAGE
    // Ref: https://redis.io/docs/latest/commands/memory-usage/
    //   "The MEMORY USAGE command reports the number of bytes that
    //    a key and its value require to be stored in RAM."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task MEMORY_USAGE_returns_byte_count_for_existing_key()
    {
        // Arrange
        await _db.StringSetAsync("mem_usage_key", "some_value_for_memory_test");

        // Act
        var result = await _db.ExecuteAsync("MEMORY", "USAGE", "mem_usage_key");

        // Assert: should return a positive integer (approximate bytes)
        Assert.False(result.IsNull);
        var bytes = (long)result;
        Assert.True(bytes > 0, $"MEMORY USAGE should return positive bytes, got {bytes}");
    }

    [Fact]
    public async Task MEMORY_USAGE_returns_null_for_nonexistent_key()
    {
        // Ref: https://redis.io/docs/latest/commands/memory-usage/
        //   "If the key does not exist, nil is returned."

        var result = await _db.ExecuteAsync("MEMORY", "USAGE", "mem_nonexistent_key");
        Assert.True(result.IsNull, "MEMORY USAGE should return nil for non-existent key");
    }

    // ═══════════════════════════════════════════════════════════════
    // DBSIZE
    // Ref: https://redis.io/docs/latest/commands/dbsize/
    //   "Return the number of keys in the currently-selected database."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task DBSIZE_returns_key_count()
    {
        // Arrange: ensure clean state, then add keys
        await _db.StringSetAsync("dbsize_k1", "v1");
        await _db.StringSetAsync("dbsize_k2", "v2");
        await _db.StringSetAsync("dbsize_k3", "v3");

        // Act
        var result = await _db.ExecuteAsync("DBSIZE");
        var size = (long)result;

        // Assert: should be at least 3 (could be more if other tests left keys)
        Assert.True(size >= 3, $"DBSIZE should be at least 3, got {size}");
    }

    // ═══════════════════════════════════════════════════════════════
    // INFO
    // Ref: https://redis.io/docs/latest/commands/info/
    //   "The INFO command returns information and statistics about
    //    the server."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task INFO_server_returns_server_section()
    {
        var result = await _db.ExecuteAsync("INFO", "server");
        Assert.False(result.IsNull);

        var info = result.ToString();
        Assert.NotNull(info);
        Assert.True(info!.Length > 0, "INFO server should return non-empty string");
        // Should contain the server section header or redis_version
        Assert.True(
            info.Contains("redis_version", StringComparison.OrdinalIgnoreCase) ||
            info.Contains("# Server", StringComparison.OrdinalIgnoreCase),
            "INFO server should contain redis_version or Server section header");
    }

    // ═══════════════════════════════════════════════════════════════
    // COMMAND COUNT
    // Ref: https://redis.io/docs/latest/commands/command-count/
    //   "Returns Integer reply of number of total commands in this
    //    Redis server."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task COMMAND_COUNT_returns_positive_integer()
    {
        var result = await _db.ExecuteAsync("COMMAND", "COUNT");
        Assert.False(result.IsNull);

        var count = (long)result;
        Assert.True(count > 0, $"COMMAND COUNT should be positive, got {count}");
    }

    // ═══════════════════════════════════════════════════════════════
    // DEBUG SLEEP
    // Ref: https://redis.io/docs/latest/commands/debug-sleep/
    //   "DEBUG SLEEP is a debugging command that suspends the server
    //    for the specified amount of time (in seconds)."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task DEBUG_SLEEP_completes_after_duration()
    {
        // Act: sleep for a short duration (0.1 seconds)
        var sw = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            var result = await _db.ExecuteAsync("DEBUG", "SLEEP", "0.1");
            sw.Stop();

            Assert.Equal("OK", result.ToString());

            // Should have taken at least ~50ms (allowing some tolerance)
            Assert.True(sw.ElapsedMilliseconds >= 50,
                $"DEBUG SLEEP 0.1 should take at least 50ms, took {sw.ElapsedMilliseconds}ms");
        }
        catch (RedisServerException ex)
        {
            // Some configurations may not support DEBUG commands
            Assert.Contains("DEBUG", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
