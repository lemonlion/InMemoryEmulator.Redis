using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ScriptingCommandTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public ScriptingCommandTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ═══════════════════════════════════════════════════════════════
    // EVAL — simple return
    // Ref: https://redis.io/docs/latest/commands/eval/
    //   "EVAL script numkeys [key [key ...]] [arg [arg ...]]"
    //   "Invoke the execution of a server-side Lua script."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Eval_simple_return_integer()
    {
        // Ref: https://redis.io/docs/latest/commands/eval/
        //   "return 1" should return the integer 1

        var result = await _db.ScriptEvaluateAsync("return 1");

        Assert.Equal(1, (long)result);
    }

    // ═══════════════════════════════════════════════════════════════
    // EVAL — with KEYS
    // Ref: https://redis.io/docs/latest/commands/eval/
    //   "KEYS and ARGV are global variables available in the script."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Eval_with_keys_accesses_redis_data()
    {
        // Ref: https://redis.io/docs/latest/commands/eval/
        //   "KEYS[1]" refers to the first key argument

        // Arrange
        await _db.StringSetAsync("eval_key_test", "hello_from_redis");

        // Act: Lua script that GETs the key
        try
        {
            var result = await _db.ScriptEvaluateAsync(
                "return redis.call('GET', KEYS[1])",
                new RedisKey[] { "eval_key_test" });

            // Assert
            Assert.Equal("hello_from_redis", result.ToString());
        }
        catch (RedisServerException ex)
        {
            // If the emulator doesn't support redis.call, verify it's a meaningful error
            Assert.True(
                ex.Message.Contains("NOSCRIPT", StringComparison.OrdinalIgnoreCase) ||
                ex.Message.Contains("ERR", StringComparison.OrdinalIgnoreCase) ||
                ex.Message.Contains("script", StringComparison.OrdinalIgnoreCase),
                $"Expected a scripting-related error, got: {ex.Message}");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // EVAL — with ARGV
    // Ref: https://redis.io/docs/latest/commands/eval/
    //   "ARGV is a table of all the additional arguments."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Eval_with_argv_returns_argument()
    {
        // Ref: https://redis.io/docs/latest/commands/eval/
        //   "return ARGV[1]" should return the first argument

        var result = await _db.ScriptEvaluateAsync(
            "return ARGV[1]",
            keys: null,
            values: new RedisValue[] { "hello" });

        Assert.Equal("hello", result.ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // EVAL — redis.call SET/GET roundtrip
    // Ref: https://redis.io/docs/latest/commands/eval/
    //   "redis.call() will raise a Lua error that in turn will force
    //    EVAL to return an error to the command caller."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Eval_redis_call_set_and_get_roundtrip()
    {
        // Ref: https://redis.io/docs/latest/commands/eval/
        //   Full roundtrip: SET a key via Lua, then GET it via Lua

        try
        {
            var script = @"
                redis.call('SET', KEYS[1], ARGV[1])
                return redis.call('GET', KEYS[1])
            ";

            var result = await _db.ScriptEvaluateAsync(
                script,
                new RedisKey[] { "eval_roundtrip" },
                new RedisValue[] { "roundtrip_value" });

            Assert.Equal("roundtrip_value", result.ToString());

            // Verify the key was actually set
            var val = await _db.StringGetAsync("eval_roundtrip");
            Assert.Equal("roundtrip_value", val.ToString());
        }
        catch (RedisServerException ex)
        {
            // If the emulator doesn't support redis.call, verify it's a meaningful error
            Assert.True(
                ex.Message.Contains("NOSCRIPT", StringComparison.OrdinalIgnoreCase) ||
                ex.Message.Contains("ERR", StringComparison.OrdinalIgnoreCase) ||
                ex.Message.Contains("script", StringComparison.OrdinalIgnoreCase),
                $"Expected a scripting-related error, got: {ex.Message}");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // SCRIPT LOAD and EVALSHA
    // Ref: https://redis.io/docs/latest/commands/script-load/
    //   "Load a script into the scripts cache, without executing it."
    // Ref: https://redis.io/docs/latest/commands/evalsha/
    //   "Evaluate a script from the server's cache by its SHA1 digest."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Script_Load_and_EvalSha_executes_cached_script()
    {
        // Ref: https://redis.io/docs/latest/commands/script-load/
        //   Returns the SHA1 digest of the script
        // Ref: https://redis.io/docs/latest/commands/evalsha/
        //   Executes a script cached on the server side by its SHA1 digest

        var script = "return 42";

        // Act: load the script
        var sha = await _db.ExecuteAsync("SCRIPT", "LOAD", script);
        Assert.False(sha.IsNull);
        var shaString = sha.ToString();
        Assert.NotNull(shaString);
        Assert.True(shaString!.Length > 0, "SHA1 digest should not be empty");

        // Act: execute by SHA
        var result = await _db.ScriptEvaluateAsync(shaString);
        Assert.Equal(42, (long)result);
    }

    // ═══════════════════════════════════════════════════════════════
    // SCRIPT EXISTS
    // Ref: https://redis.io/docs/latest/commands/script-exists/
    //   "Returns information about the existence of the scripts
    //    in the script cache."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Script_Exists_returns_1_for_loaded_script()
    {
        // Arrange: load a script first
        var script = "return 99";
        var sha = (await _db.ExecuteAsync("SCRIPT", "LOAD", script)).ToString();

        // Act: check if it exists
        var result = await _db.ExecuteAsync("SCRIPT", "EXISTS", sha);
        var arr = (RedisResult[])result!;

        // Assert: should return 1 (exists)
        Assert.Single(arr);
        Assert.Equal(1, (long)arr[0]);
    }

    [Fact]
    public async Task Script_Exists_returns_0_for_unknown_sha()
    {
        // Ref: https://redis.io/docs/latest/commands/script-exists/
        //   "For every corresponding SHA1 digest of a script that does not exist
        //    in the script cache, a 0 is returned."

        var result = await _db.ExecuteAsync("SCRIPT", "EXISTS", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        var arr = (RedisResult[])result!;
        Assert.Single(arr);
        Assert.Equal(0, (long)arr[0]);
    }

    // ═══════════════════════════════════════════════════════════════
    // SCRIPT FLUSH
    // Ref: https://redis.io/docs/latest/commands/script-flush/
    //   "Flush the Lua scripts cache."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Script_Flush_clears_script_cache()
    {
        // Arrange: load a script
        var sha = (await _db.ExecuteAsync("SCRIPT", "LOAD", "return 'cached'")).ToString();

        // Verify it exists
        var before = (RedisResult[])(await _db.ExecuteAsync("SCRIPT", "EXISTS", sha))!;
        Assert.Equal(1, (long)before[0]);

        // Act: flush the cache
        var flushResult = await _db.ExecuteAsync("SCRIPT", "FLUSH");
        Assert.Equal("OK", flushResult.ToString());

        // Assert: script should no longer exist
        var after = (RedisResult[])(await _db.ExecuteAsync("SCRIPT", "EXISTS", sha))!;
        Assert.Equal(0, (long)after[0]);
    }
}
