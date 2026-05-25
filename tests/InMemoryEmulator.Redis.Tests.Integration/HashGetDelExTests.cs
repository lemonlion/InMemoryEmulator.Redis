using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

/// <summary>
/// Tests for HGETDEL, HGETEX, and HSETEX — Redis 8.0+ commands.
/// Ref: https://redis.io/docs/latest/commands/hgetdel/
/// Ref: https://redis.io/docs/latest/commands/hgetex/
/// Ref: https://redis.io/docs/latest/commands/hsetex/
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class HashGetDelExTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public HashGetDelExTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ============================================================
    // HGETDEL tests
    // Ref: https://redis.io/docs/latest/commands/hgetdel/
    // ============================================================

    [Fact]
    public async Task HGetDel_returns_values_and_removes_fields()
    {
        await _db.HashSetAsync("hgd1", new HashEntry[] { new("f1", "v1"), new("f2", "v2"), new("f3", "v3") });

        var result = await _db.ExecuteAsync("HGETDEL", "hgd1", "FIELDS", "2", "f1", "f2");
        var arr = (RedisResult[])result!;
        Assert.Equal(2, arr.Length);
        Assert.Equal("v1", arr[0].ToString());
        Assert.Equal("v2", arr[1].ToString());

        // f1 and f2 should be gone
        Assert.True((await _db.HashGetAsync("hgd1", "f1")).IsNull);
        Assert.True((await _db.HashGetAsync("hgd1", "f2")).IsNull);
        // f3 should remain
        Assert.Equal("v3", (await _db.HashGetAsync("hgd1", "f3")).ToString());
    }

    [Fact]
    public async Task HGetDel_returns_nil_for_missing_fields()
    {
        await _db.HashSetAsync("hgd2", "f1", "v1");

        var result = await _db.ExecuteAsync("HGETDEL", "hgd2", "FIELDS", "2", "f1", "missing");
        var arr = (RedisResult[])result!;
        Assert.Equal("v1", arr[0].ToString());
        Assert.True(arr[1].IsNull);
    }

    [Fact]
    public async Task HGetDel_on_nonexistent_key_returns_nils()
    {
        var result = await _db.ExecuteAsync("HGETDEL", "hgd_nokey", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Single(arr);
        Assert.True(arr[0].IsNull);
    }

    [Fact]
    public async Task HGetDel_removes_last_field_deletes_key()
    {
        await _db.HashSetAsync("hgd_last", "f1", "v1");

        await _db.ExecuteAsync("HGETDEL", "hgd_last", "FIELDS", "1", "f1");

        Assert.False(await _db.KeyExistsAsync("hgd_last"));
    }

    [Fact]
    public async Task HGetDel_does_not_return_expired_fields()
    {
        await _db.HashSetAsync("hgd_exp", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        await _db.ExecuteAsync("HPEXPIRE", "hgd_exp", "100", "FIELDS", "1", "f1");

        await Task.Delay(200);

        var result = await _db.ExecuteAsync("HGETDEL", "hgd_exp", "FIELDS", "2", "f1", "f2");
        var arr = (RedisResult[])result!;
        Assert.True(arr[0].IsNull);   // f1 expired
        Assert.Equal("v2", arr[1].ToString()); // f2 still there
    }

    // ============================================================
    // HGETEX tests
    // Ref: https://redis.io/docs/latest/commands/hgetex/
    // ============================================================

    [Fact]
    public async Task HGetEx_without_expiry_option_returns_values()
    {
        await _db.HashSetAsync("hge1", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });

        var result = await _db.ExecuteAsync("HGETEX", "hge1", "FIELDS", "2", "f1", "f2");
        var arr = (RedisResult[])result!;
        Assert.Equal("v1", arr[0].ToString());
        Assert.Equal("v2", arr[1].ToString());
    }

    [Fact]
    public async Task HGetEx_with_EX_sets_expiry_and_returns_values()
    {
        await _db.HashSetAsync("hge_ex", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });

        var result = await _db.ExecuteAsync("HGETEX", "hge_ex", "EX", "60", "FIELDS", "2", "f1", "f2");
        var arr = (RedisResult[])result!;
        Assert.Equal("v1", arr[0].ToString());
        Assert.Equal("v2", arr[1].ToString());

        // Verify TTL was set
        var ttlResult = await _db.ExecuteAsync("HTTL", "hge_ex", "FIELDS", "1", "f1");
        var ttlArr = (RedisResult[])ttlResult!;
        Assert.InRange((long)ttlArr[0], 50, 60);
    }

    [Fact]
    public async Task HGetEx_with_PX_sets_expiry_in_milliseconds()
    {
        await _db.HashSetAsync("hge_px", "f1", "v1");

        var result = await _db.ExecuteAsync("HGETEX", "hge_px", "PX", "60000", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal("v1", arr[0].ToString());

        var pttlResult = await _db.ExecuteAsync("HPTTL", "hge_px", "FIELDS", "1", "f1");
        var pttlArr = (RedisResult[])pttlResult!;
        Assert.InRange((long)pttlArr[0], 50000, 60000);
    }

    [Fact]
    public async Task HGetEx_with_EXAT_sets_absolute_expiry()
    {
        await _db.HashSetAsync("hge_exat", "f1", "v1");
        var futureTs = DateTimeOffset.UtcNow.AddSeconds(60).ToUnixTimeSeconds();

        var result = await _db.ExecuteAsync("HGETEX", "hge_exat", "EXAT", futureTs.ToString(), "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal("v1", arr[0].ToString());

        var etResult = await _db.ExecuteAsync("HEXPIRETIME", "hge_exat", "FIELDS", "1", "f1");
        var etArr = (RedisResult[])etResult!;
        Assert.InRange((long)etArr[0], futureTs - 2, futureTs + 2);
    }

    [Fact]
    public async Task HGetEx_with_PXAT_sets_absolute_expiry_millis()
    {
        await _db.HashSetAsync("hge_pxat", "f1", "v1");
        var futureMs = DateTimeOffset.UtcNow.AddSeconds(60).ToUnixTimeMilliseconds();

        var result = await _db.ExecuteAsync("HGETEX", "hge_pxat", "PXAT", futureMs.ToString(), "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal("v1", arr[0].ToString());
    }

    [Fact]
    public async Task HGetEx_with_PERSIST_removes_expiry()
    {
        await _db.HashSetAsync("hge_persist", "f1", "v1");
        await _db.ExecuteAsync("HEXPIRE", "hge_persist", "60", "FIELDS", "1", "f1");

        // Verify expiry is set
        var ttlResult = await _db.ExecuteAsync("HTTL", "hge_persist", "FIELDS", "1", "f1");
        var ttlArr = (RedisResult[])ttlResult!;
        Assert.True((long)ttlArr[0] > 0);

        // HGETEX with PERSIST
        var result = await _db.ExecuteAsync("HGETEX", "hge_persist", "PERSIST", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal("v1", arr[0].ToString());

        // Verify expiry removed
        ttlResult = await _db.ExecuteAsync("HTTL", "hge_persist", "FIELDS", "1", "f1");
        ttlArr = (RedisResult[])ttlResult!;
        Assert.Equal(-1, (long)ttlArr[0]);
    }

    [Fact]
    public async Task HGetEx_returns_nil_for_missing_fields()
    {
        await _db.HashSetAsync("hge_nil", "f1", "v1");

        var result = await _db.ExecuteAsync("HGETEX", "hge_nil", "FIELDS", "2", "f1", "missing");
        var arr = (RedisResult[])result!;
        Assert.Equal("v1", arr[0].ToString());
        Assert.True(arr[1].IsNull);
    }

    [Fact]
    public async Task HGetEx_on_nonexistent_key_returns_nils()
    {
        var result = await _db.ExecuteAsync("HGETEX", "hge_nokey", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.True(arr[0].IsNull);
    }

    // ============================================================
    // HSETEX tests
    // Ref: https://redis.io/docs/latest/commands/hsetex/
    // ============================================================

    [Fact]
    public async Task HSetEx_sets_fields_with_expiry_in_seconds()
    {
        // Ref: https://redis.io/docs/latest/commands/hsetex/
        //   Returns 0 if no fields were set, 1 if all the fields were set.
        var result = await _db.ExecuteAsync("HSETEX", "hse_ex", "EX", "60", "FIELDS", "2", "f1", "v1", "f2", "v2");
        Assert.Equal(1, (long)result);

        Assert.Equal("v1", (await _db.HashGetAsync("hse_ex", "f1")).ToString());
        Assert.Equal("v2", (await _db.HashGetAsync("hse_ex", "f2")).ToString());

        var ttlResult = await _db.ExecuteAsync("HTTL", "hse_ex", "FIELDS", "1", "f1");
        var ttlArr = (RedisResult[])ttlResult!;
        Assert.InRange((long)ttlArr[0], 50, 60);
    }

    [Fact]
    public async Task HSetEx_sets_fields_with_expiry_in_milliseconds()
    {
        await _db.ExecuteAsync("HSETEX", "hse_px", "PX", "60000", "FIELDS", "1", "f1", "v1");

        var pttlResult = await _db.ExecuteAsync("HPTTL", "hse_px", "FIELDS", "1", "f1");
        var pttlArr = (RedisResult[])pttlResult!;
        Assert.InRange((long)pttlArr[0], 50000, 60000);
    }

    [Fact]
    public async Task HSetEx_sets_fields_with_absolute_timestamp()
    {
        var futureTs = DateTimeOffset.UtcNow.AddSeconds(60).ToUnixTimeSeconds();
        await _db.ExecuteAsync("HSETEX", "hse_exat", "EXAT", futureTs.ToString(), "FIELDS", "1", "f1", "v1");

        var etResult = await _db.ExecuteAsync("HEXPIRETIME", "hse_exat", "FIELDS", "1", "f1");
        var etArr = (RedisResult[])etResult!;
        Assert.InRange((long)etArr[0], futureTs - 2, futureTs + 2);
    }

    [Fact]
    public async Task HSetEx_with_keepttl_preserves_existing_expiry()
    {
        // Ref: https://redis.io/docs/latest/commands/hsetex/
        //   KEEPTTL retains the existing per-field TTL when overwriting.
        await _db.ExecuteAsync("HSETEX", "hse_kt", "EX", "60", "FIELDS", "1", "f1", "v1");
        var result = await _db.ExecuteAsync("HSETEX", "hse_kt", "KEEPTTL", "FIELDS", "1", "f1", "v2");
        Assert.Equal(1, (long)result);

        Assert.Equal("v2", (await _db.HashGetAsync("hse_kt", "f1")).ToString());

        var ttlResult = await _db.ExecuteAsync("HTTL", "hse_kt", "FIELDS", "1", "f1");
        var ttlArr = (RedisResult[])ttlResult!;
        Assert.InRange((long)ttlArr[0], 50, 60);
    }

    [Fact]
    public async Task HSetEx_returns_one_on_success()
    {
        // Ref: https://redis.io/docs/latest/commands/hsetex/
        //   Returns 0 if no fields were set, 1 if all the fields were set.
        await _db.HashSetAsync("hse_count", "f1", "old");

        var result = await _db.ExecuteAsync("HSETEX", "hse_count", "EX", "60", "FIELDS", "2", "f1", "new_val", "f2", "v2");
        Assert.Equal(1, (long)result);
    }

    [Fact]
    public async Task HSetEx_FNX_only_sets_new_fields()
    {
        // Ref: https://redis.io/docs/latest/commands/hsetex/
        //   FNX: Only set new fields (skip existing fields).
        await _db.HashSetAsync("hse_fnx", "f1", "old");

        var result = await _db.ExecuteAsync("HSETEX", "hse_fnx", "FNX", "EX", "60", "FIELDS", "2", "f1", "new_val", "f2", "v2");
        Assert.Equal(1, (long)result);

        Assert.Equal("old", (await _db.HashGetAsync("hse_fnx", "f1")).ToString());
        Assert.Equal("v2", (await _db.HashGetAsync("hse_fnx", "f2")).ToString());
    }

    [Fact]
    public async Task HSetEx_FXX_only_sets_existing_fields()
    {
        // Ref: https://redis.io/docs/latest/commands/hsetex/
        //   FXX: Only set existing fields (skip new fields).
        await _db.HashSetAsync("hse_fxx", "f1", "old");

        var result = await _db.ExecuteAsync("HSETEX", "hse_fxx", "FXX", "EX", "60", "FIELDS", "2", "f1", "new_val", "f2", "v2");
        Assert.Equal(1, (long)result);

        Assert.Equal("new_val", (await _db.HashGetAsync("hse_fxx", "f1")).ToString());
        Assert.True((await _db.HashGetAsync("hse_fxx", "f2")).IsNull);
    }
}
