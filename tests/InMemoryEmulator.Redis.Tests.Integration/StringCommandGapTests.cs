using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

/// <summary>
/// Tests covering missing string command scenarios for Redis parity.
/// Covers GETSET, MSETNX, SETEX, PSETEX, GETEX (EX/PX/EXAT/PXAT),
/// LCS, SUBSTR, IncrByFloat formatting, SET KEEPTTL/EXAT/PXAT.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class StringCommandGapTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public StringCommandGapTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ========================================================================
    // 1. GETSET (deprecated but still supported) — returns old value, sets new
    // Ref: https://redis.io/docs/latest/commands/getset/
    //   "Atomically sets key to value and returns the old value stored at key."
    // ========================================================================

    [Fact]
    public async Task GetSet_returns_old_value_and_sets_new()
    {
        await _db.StringSetAsync("getset_key", "old_value");
        var previous = await _db.StringGetSetAsync("getset_key", "new_value");
        Assert.Equal("old_value", previous.ToString());

        var current = await _db.StringGetAsync("getset_key");
        Assert.Equal("new_value", current.ToString());
    }

    // ========================================================================
    // 12. GETSET on nonexistent key — returns nil, creates key
    // Ref: https://redis.io/docs/latest/commands/getset/
    //   "Returns the old value stored at key, or nil when key did not exist."
    // ========================================================================

    [Fact]
    public async Task GetSet_on_nonexistent_key_returns_nil_and_creates_key()
    {
        var previous = await _db.StringGetSetAsync("getset_missing", "created_value");
        Assert.True(previous.IsNull);

        var current = await _db.StringGetAsync("getset_missing");
        Assert.Equal("created_value", current.ToString());
    }

    // ========================================================================
    // 2. MSETNX — only sets if NONE of the keys exist, returns bool
    // Ref: https://redis.io/docs/latest/commands/msetnx/
    //   "Sets the given keys to their respective values. MSETNX will not
    //    perform any operation at all even if just a single key already exists."
    // ========================================================================

    [Fact]
    public async Task MSetNx_sets_all_when_none_exist()
    {
        var result = await _db.StringSetAsync(
            new KeyValuePair<RedisKey, RedisValue>[]
            {
                new("msetnx_a", "val_a"),
                new("msetnx_b", "val_b"),
                new("msetnx_c", "val_c")
            },
            When.NotExists);

        Assert.True(result);
        Assert.Equal("val_a", (await _db.StringGetAsync("msetnx_a")).ToString());
        Assert.Equal("val_b", (await _db.StringGetAsync("msetnx_b")).ToString());
        Assert.Equal("val_c", (await _db.StringGetAsync("msetnx_c")).ToString());
    }

    // ========================================================================
    // 13. MSETNX when some keys exist — returns false, sets nothing (atomic)
    // Ref: https://redis.io/docs/latest/commands/msetnx/
    //   "MSETNX is atomic, so all given keys are set at once."
    //   "It will not perform any operation at all even if just a single key
    //    already exists."
    // ========================================================================

    [Fact]
    public async Task MSetNx_when_some_keys_exist_returns_false_and_sets_nothing()
    {
        // Pre-set one of the keys
        await _db.StringSetAsync("msetnx_exists_b", "pre_existing");

        var result = await _db.StringSetAsync(
            new KeyValuePair<RedisKey, RedisValue>[]
            {
                new("msetnx_exists_a", "val_a"),
                new("msetnx_exists_b", "val_b"),
                new("msetnx_exists_c", "val_c")
            },
            When.NotExists);

        Assert.False(result);

        // None of the keys should have been set/changed
        Assert.False(await _db.KeyExistsAsync("msetnx_exists_a"));
        Assert.Equal("pre_existing", (await _db.StringGetAsync("msetnx_exists_b")).ToString());
        Assert.False(await _db.KeyExistsAsync("msetnx_exists_c"));
    }

    // ========================================================================
    // 3. SETEX — set with expiry in seconds
    // Ref: https://redis.io/docs/latest/commands/setex/
    //   "Set key to hold the string value and set key to timeout after a given
    //    number of seconds."
    // ========================================================================

    [Fact]
    public async Task SetEx_sets_value_with_seconds_expiry()
    {
        await _db.StringSetAsync("setex_key", "value", TimeSpan.FromSeconds(100));

        var value = await _db.StringGetAsync("setex_key");
        Assert.Equal("value", value.ToString());

        var ttl = await _db.KeyTimeToLiveAsync("setex_key");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalSeconds, 90, 100);
    }

    [Fact]
    public async Task SetEx_overwrites_existing_value_and_ttl()
    {
        await _db.StringSetAsync("setex_over", "old", TimeSpan.FromSeconds(200));
        await _db.StringSetAsync("setex_over", "new", TimeSpan.FromSeconds(50));

        Assert.Equal("new", (await _db.StringGetAsync("setex_over")).ToString());
        var ttl = await _db.KeyTimeToLiveAsync("setex_over");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalSeconds, 40, 50);
    }

    // ========================================================================
    // 4. PSETEX — set with expiry in milliseconds
    // Ref: https://redis.io/docs/latest/commands/psetex/
    //   "PSETEX works exactly like SETEX with the sole difference that the
    //    expire time is specified in milliseconds instead of seconds."
    // ========================================================================

    [Fact]
    public async Task PSetEx_sets_value_with_millisecond_expiry()
    {
        await _db.StringSetAsync("psetex_key", "value", TimeSpan.FromMilliseconds(5000));

        var value = await _db.StringGetAsync("psetex_key");
        Assert.Equal("value", value.ToString());

        var ttl = await _db.KeyTimeToLiveAsync("psetex_key");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalMilliseconds, 4000, 5000);
    }

    [Fact]
    public async Task PSetEx_millisecond_precision_expires_correctly()
    {
        // Set a short TTL to verify ms precision
        await _db.StringSetAsync("psetex_short", "value", TimeSpan.FromMilliseconds(200));
        Assert.False((await _db.StringGetAsync("psetex_short")).IsNull);

        await Task.Delay(300);
        Assert.True((await _db.StringGetAsync("psetex_short")).IsNull);
    }

    // ========================================================================
    // 5. GETEX with EX — get value and set expiry in seconds
    // Ref: https://redis.io/docs/latest/commands/getex/
    //   "Get the value of key and optionally set its expiration."
    //   "EX seconds -- Set the specified expire time, in seconds."
    // ========================================================================

    [Fact]
    public async Task GetEx_with_EX_returns_value_and_sets_seconds_expiry()
    {
        await _db.StringSetAsync("getex_ex", "hello");

        // No expiry initially
        var ttlBefore = await _db.KeyTimeToLiveAsync("getex_ex");
        Assert.Null(ttlBefore);

        var result = await _db.ExecuteAsync("GETEX", "getex_ex", "EX", "100");
        Assert.Equal("hello", result.ToString());

        var ttlAfter = await _db.KeyTimeToLiveAsync("getex_ex");
        Assert.NotNull(ttlAfter);
        Assert.InRange(ttlAfter!.Value.TotalSeconds, 90, 100);
    }

    [Fact]
    public async Task GetEx_on_nonexistent_key_returns_nil()
    {
        var result = await _db.ExecuteAsync("GETEX", "getex_missing", "EX", "100");
        Assert.True(result.IsNull);
    }

    // ========================================================================
    // 6. GETEX with PX — get with millisecond expiry
    // Ref: https://redis.io/docs/latest/commands/getex/
    //   "PX milliseconds -- Set the specified expire time, in milliseconds."
    // ========================================================================

    [Fact]
    public async Task GetEx_with_PX_returns_value_and_sets_millisecond_expiry()
    {
        await _db.StringSetAsync("getex_px", "world");

        var result = await _db.ExecuteAsync("GETEX", "getex_px", "PX", "5000");
        Assert.Equal("world", result.ToString());

        var ttl = await _db.KeyTimeToLiveAsync("getex_px");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalMilliseconds, 4000, 5000);
    }

    // ========================================================================
    // 7. GETEX with EXAT — get with absolute Unix timestamp expiry (seconds)
    // Ref: https://redis.io/docs/latest/commands/getex/
    //   "EXAT timestamp-seconds -- Set the specified Unix time at which the
    //    key will expire, in seconds."
    // ========================================================================

    [Fact]
    public async Task GetEx_with_EXAT_sets_absolute_expiry_in_seconds()
    {
        await _db.StringSetAsync("getex_exat", "data");

        var futureTimestamp = DateTimeOffset.UtcNow.AddSeconds(120).ToUnixTimeSeconds();
        var result = await _db.ExecuteAsync("GETEX", "getex_exat", "EXAT", futureTimestamp.ToString());
        Assert.Equal("data", result.ToString());

        var ttl = await _db.KeyTimeToLiveAsync("getex_exat");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalSeconds, 100, 120);
    }

    // ========================================================================
    // 8. GETEX with PXAT — get with absolute Unix timestamp expiry (ms)
    // Ref: https://redis.io/docs/latest/commands/getex/
    //   "PXAT timestamp-milliseconds -- Set the specified Unix time at which
    //    the key will expire, in milliseconds."
    // ========================================================================

    [Fact]
    public async Task GetEx_with_PXAT_sets_absolute_expiry_in_milliseconds()
    {
        await _db.StringSetAsync("getex_pxat", "info");

        var futureMs = DateTimeOffset.UtcNow.AddSeconds(120).ToUnixTimeMilliseconds();
        var result = await _db.ExecuteAsync("GETEX", "getex_pxat", "PXAT", futureMs.ToString());
        Assert.Equal("info", result.ToString());

        var ttl = await _db.KeyTimeToLiveAsync("getex_pxat");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalSeconds, 100, 120);
    }

    // ========================================================================
    // 9. LCS (Longest Common Substring)
    // Ref: https://redis.io/docs/latest/commands/lcs/
    //   "Returns the longest common substring."
    // ========================================================================

    [Fact]
    public async Task Lcs_returns_longest_common_substring()
    {
        await _db.StringSetAsync("lcs_a", "ohmytext");
        await _db.StringSetAsync("lcs_b", "mynewtext");

        var result = await _db.ExecuteAsync("LCS", "lcs_a", "lcs_b");
        Assert.Equal("mytext", result.ToString());
    }

    [Fact]
    public async Task Lcs_with_no_common_substring_returns_empty()
    {
        await _db.StringSetAsync("lcs_no_a", "abc");
        await _db.StringSetAsync("lcs_no_b", "xyz");

        var result = await _db.ExecuteAsync("LCS", "lcs_no_a", "lcs_no_b");
        Assert.Equal("", result.ToString());
    }

    [Fact]
    public async Task Lcs_with_identical_strings_returns_full_string()
    {
        await _db.StringSetAsync("lcs_id_a", "identical");
        await _db.StringSetAsync("lcs_id_b", "identical");

        var result = await _db.ExecuteAsync("LCS", "lcs_id_a", "lcs_id_b");
        Assert.Equal("identical", result.ToString());
    }

    // ========================================================================
    // 10. LCS with LEN — returns length of LCS
    // Ref: https://redis.io/docs/latest/commands/lcs/
    //   "LEN: return the length of the match."
    // ========================================================================

    [Fact]
    public async Task Lcs_with_LEN_returns_length()
    {
        await _db.StringSetAsync("lcs_len_a", "ohmytext");
        await _db.StringSetAsync("lcs_len_b", "mynewtext");

        var result = await _db.ExecuteAsync("LCS", "lcs_len_a", "lcs_len_b", "LEN");
        Assert.Equal(6, (long)result); // "mytext" = 6 chars
    }

    [Fact]
    public async Task Lcs_with_LEN_no_common_returns_zero()
    {
        await _db.StringSetAsync("lcs_len0_a", "abc");
        await _db.StringSetAsync("lcs_len0_b", "xyz");

        var result = await _db.ExecuteAsync("LCS", "lcs_len0_a", "lcs_len0_b", "LEN");
        Assert.Equal(0, (long)result);
    }

    // ========================================================================
    // 11. SUBSTR (alias for GETRANGE)
    // Ref: https://redis.io/docs/latest/commands/substr/
    //   "Returns the substring of the string value stored at key, determined
    //    by the offsets start and end (both are inclusive)."
    // ========================================================================

    [Fact]
    public async Task Substr_returns_substring()
    {
        await _db.StringSetAsync("substr_key", "Hello, World!");

        var result = await _db.ExecuteAsync("SUBSTR", "substr_key", "0", "4");
        Assert.Equal("Hello", result.ToString());
    }

    [Fact]
    public async Task Substr_with_negative_indices()
    {
        await _db.StringSetAsync("substr_neg", "Hello, World!");

        var result = await _db.ExecuteAsync("SUBSTR", "substr_neg", "-6", "-1");
        Assert.Equal("World!", result.ToString());
    }

    [Fact]
    public async Task Substr_on_nonexistent_key_returns_empty()
    {
        var result = await _db.ExecuteAsync("SUBSTR", "substr_missing", "0", "10");
        Assert.Equal("", result.ToString());
    }

    [Fact]
    public async Task Substr_beyond_length_returns_available()
    {
        await _db.StringSetAsync("substr_short", "Hi");

        var result = await _db.ExecuteAsync("SUBSTR", "substr_short", "0", "100");
        Assert.Equal("Hi", result.ToString());
    }

    // ========================================================================
    // 14. IncrByFloat formatting — Redis strips trailing zeros
    // Ref: https://redis.io/docs/latest/commands/incrbyfloat/
    //   "The value is stored as a string representation of a floating point
    //    number. Both the value already contained in the string key and the
    //    increment value can be optionally provided in exponential notation,
    //    however the value computed after the increment is always stored in
    //    the same format, that is, an integer number followed (when needed)
    //    by a dot, and a variable number of digits representing the decimal
    //    part of the number. Trailing zeroes are always removed."
    // ========================================================================

    [Fact]
    public async Task IncrByFloat_strips_trailing_zeros()
    {
        await _db.StringSetAsync("incrf_fmt", "10.50");
        await _db.StringIncrementAsync("incrf_fmt", 1.0e2);

        // 10.50 + 100.0 = 110.5 — Redis stores "110.5" not "110.50" or "110.500..."
        var stored = (await _db.StringGetAsync("incrf_fmt")).ToString();
        Assert.Equal("110.5", stored);
    }

    [Fact]
    public async Task IncrByFloat_integer_result_has_no_decimal()
    {
        await _db.StringSetAsync("incrf_int_result", "3.0");
        await _db.StringIncrementAsync("incrf_int_result", 7.0);

        // 3.0 + 7.0 = 10 — Redis stores "10" (integer representation, no trailing .0)
        var stored = (await _db.StringGetAsync("incrf_int_result")).ToString();
        Assert.Equal("10", stored);
    }

    [Fact]
    public async Task IncrByFloat_preserves_necessary_decimals()
    {
        // Ref: https://redis.io/docs/latest/commands/incrbyfloat/
        //   Trailing zeroes are always removed, but all significant digits are preserved.
        //   Note: 3.1415 cannot be represented exactly in IEEE 754 double, so Redis
        //   (which uses long double internally) may store "3.1415000000000001" or similar.
        //   We verify the value round-trips correctly as a double.
        await _db.StringSetAsync("incrf_dec", "0");
        var result = await _db.StringIncrementAsync("incrf_dec", 3.1415);

        Assert.Equal(3.1415, result, precision: 10);

        // Verify stored string has no trailing zeros (beyond precision noise)
        var stored = (await _db.StringGetAsync("incrf_dec")).ToString();
        Assert.False(stored!.EndsWith("0") && stored.Contains('.'),
            $"Stored value '{stored}' should not have trailing zeros after decimal point");
    }

    // ========================================================================
    // 15. SET with KEEPTTL — preserves existing TTL when overwriting
    // Ref: https://redis.io/docs/latest/commands/set/
    //   "KEEPTTL -- Retain the time to live associated with the key."
    // ========================================================================

    [Fact]
    public async Task Set_with_KEEPTTL_preserves_existing_ttl()
    {
        await _db.StringSetAsync("keepttl_key", "original", TimeSpan.FromSeconds(100));

        // Overwrite value but keep TTL
        await _db.ExecuteAsync("SET", "keepttl_key", "updated", "KEEPTTL");

        Assert.Equal("updated", (await _db.StringGetAsync("keepttl_key")).ToString());

        var ttl = await _db.KeyTimeToLiveAsync("keepttl_key");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalSeconds, 80, 100);
    }

    [Fact]
    public async Task Set_with_KEEPTTL_on_key_without_ttl_keeps_no_ttl()
    {
        await _db.StringSetAsync("keepttl_no_exp", "original");

        await _db.ExecuteAsync("SET", "keepttl_no_exp", "updated", "KEEPTTL");

        Assert.Equal("updated", (await _db.StringGetAsync("keepttl_no_exp")).ToString());

        var ttl = await _db.KeyTimeToLiveAsync("keepttl_no_exp");
        Assert.Null(ttl);
    }

    // ========================================================================
    // 16. SET with EXAT — absolute Unix timestamp expiry in seconds
    // Ref: https://redis.io/docs/latest/commands/set/
    //   "EXAT timestamp-seconds -- Set the specified Unix time at which the
    //    key will expire, in seconds (a positive integer)."
    // ========================================================================

    [Fact]
    public async Task Set_with_EXAT_sets_absolute_seconds_expiry()
    {
        var futureTimestamp = DateTimeOffset.UtcNow.AddSeconds(120).ToUnixTimeSeconds();

        await _db.ExecuteAsync("SET", "set_exat_key", "value", "EXAT", futureTimestamp.ToString());

        Assert.Equal("value", (await _db.StringGetAsync("set_exat_key")).ToString());

        var ttl = await _db.KeyTimeToLiveAsync("set_exat_key");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalSeconds, 100, 120);
    }

    // ========================================================================
    // 17. SET with PXAT — absolute Unix timestamp expiry in milliseconds
    // Ref: https://redis.io/docs/latest/commands/set/
    //   "PXAT timestamp-milliseconds -- Set the specified Unix time at which
    //    the key will expire, in milliseconds (a positive integer)."
    // ========================================================================

    [Fact]
    public async Task Set_with_PXAT_sets_absolute_milliseconds_expiry()
    {
        var futureMs = DateTimeOffset.UtcNow.AddSeconds(120).ToUnixTimeMilliseconds();

        await _db.ExecuteAsync("SET", "set_pxat_key", "value", "PXAT", futureMs.ToString());

        Assert.Equal("value", (await _db.StringGetAsync("set_pxat_key")).ToString());

        var ttl = await _db.KeyTimeToLiveAsync("set_pxat_key");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalSeconds, 100, 120);
    }

    [Fact]
    public async Task Set_with_PXAT_in_past_makes_key_not_exist()
    {
        // A PXAT in the past should result in the key not existing
        // (Redis treats past expiry as immediate expiration)
        var pastMs = DateTimeOffset.UtcNow.AddSeconds(-10).ToUnixTimeMilliseconds();

        await _db.ExecuteAsync("SET", "set_pxat_past", "value", "PXAT", pastMs.ToString());

        Assert.False(await _db.KeyExistsAsync("set_pxat_past"));
    }

    [Fact]
    public async Task Set_with_EXAT_in_past_makes_key_not_exist()
    {
        // Same for EXAT: a timestamp in the past means key is immediately expired
        var pastTimestamp = DateTimeOffset.UtcNow.AddSeconds(-10).ToUnixTimeSeconds();

        await _db.ExecuteAsync("SET", "set_exat_past", "value", "EXAT", pastTimestamp.ToString());

        Assert.False(await _db.KeyExistsAsync("set_exat_past"));
    }
}
