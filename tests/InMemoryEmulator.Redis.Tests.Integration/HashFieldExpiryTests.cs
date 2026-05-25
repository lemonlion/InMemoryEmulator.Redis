using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

/// <summary>
/// Tests for Redis 7.4 per-field hash expiration commands.
/// Ref: https://redis.io/docs/latest/develop/data-types/hashes/#field-expiration
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class HashFieldExpiryTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public HashFieldExpiryTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ============================================================
    // HEXPIRE tests
    // Ref: https://redis.io/docs/latest/commands/hexpire/
    // ============================================================

    [Fact]
    public async Task HExpire_sets_field_ttl_in_seconds()
    {
        await _db.HashSetAsync("hexp1", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });

        var result = await _db.ExecuteAsync("HEXPIRE", "hexp1", "60", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Single(arr);
        Assert.Equal(1, (long)arr[0]); // 1 = expiry set
    }

    [Fact]
    public async Task HExpire_returns_negative2_for_nonexistent_field()
    {
        await _db.HashSetAsync("hexp2", "f1", "v1");

        var result = await _db.ExecuteAsync("HEXPIRE", "hexp2", "60", "FIELDS", "1", "missing");
        var arr = (RedisResult[])result!;
        Assert.Equal(-2, (long)arr[0]); // -2 = no such field
    }

    [Fact]
    public async Task HExpire_returns_negative2_for_nonexistent_key()
    {
        var result = await _db.ExecuteAsync("HEXPIRE", "hexp_nokey", "60", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(-2, (long)arr[0]);
    }

    [Fact]
    public async Task HExpire_multiple_fields_returns_per_field_results()
    {
        await _db.HashSetAsync("hexp3", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });

        var result = await _db.ExecuteAsync("HEXPIRE", "hexp3", "60", "FIELDS", "3", "f1", "f2", "missing");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal(1, (long)arr[0]);  // f1 = set
        Assert.Equal(1, (long)arr[1]);  // f2 = set
        Assert.Equal(-2, (long)arr[2]); // missing = no such field
    }

    [Fact]
    public async Task HExpire_field_becomes_inaccessible_after_expiry()
    {
        await _db.HashSetAsync("hexp_die", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        await _db.ExecuteAsync("HEXPIRE", "hexp_die", "1", "FIELDS", "1", "f1");

        // f1 should still be accessible right now
        var val = await _db.HashGetAsync("hexp_die", "f1");
        Assert.False(val.IsNull);

        // Wait for expiry
        await Task.Delay(1500);

        // f1 should be gone, f2 should remain
        val = await _db.HashGetAsync("hexp_die", "f1");
        Assert.True(val.IsNull);
        val = await _db.HashGetAsync("hexp_die", "f2");
        Assert.Equal("v2", val.ToString());
    }

    [Fact]
    public async Task HExpire_with_NX_only_sets_if_no_expiry()
    {
        await _db.HashSetAsync("hexp_nx", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        // Set expiry on f1
        await _db.ExecuteAsync("HEXPIRE", "hexp_nx", "100", "FIELDS", "1", "f1");

        // NX should fail for f1 (already has expiry), succeed for f2
        var result = await _db.ExecuteAsync("HEXPIRE", "hexp_nx", "200", "NX", "FIELDS", "2", "f1", "f2");
        var arr = (RedisResult[])result!;
        Assert.Equal(0, (long)arr[0]); // f1: condition not met
        Assert.Equal(1, (long)arr[1]); // f2: set
    }

    [Fact]
    public async Task HExpire_with_XX_only_sets_if_has_expiry()
    {
        await _db.HashSetAsync("hexp_xx", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        await _db.ExecuteAsync("HEXPIRE", "hexp_xx", "100", "FIELDS", "1", "f1");

        // XX should succeed for f1 (has expiry), fail for f2 (no expiry)
        var result = await _db.ExecuteAsync("HEXPIRE", "hexp_xx", "200", "XX", "FIELDS", "2", "f1", "f2");
        var arr = (RedisResult[])result!;
        Assert.Equal(1, (long)arr[0]); // f1: updated
        Assert.Equal(0, (long)arr[1]); // f2: condition not met
    }

    [Fact]
    public async Task HExpire_with_GT_only_sets_if_new_expiry_greater()
    {
        await _db.HashSetAsync("hexp_gt", "f1", "v1");
        await _db.ExecuteAsync("HEXPIRE", "hexp_gt", "100", "FIELDS", "1", "f1");

        // GT with smaller expiry should fail
        var result = await _db.ExecuteAsync("HEXPIRE", "hexp_gt", "50", "GT", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(0, (long)arr[0]);

        // GT with larger expiry should succeed
        result = await _db.ExecuteAsync("HEXPIRE", "hexp_gt", "200", "GT", "FIELDS", "1", "f1");
        arr = (RedisResult[])result!;
        Assert.Equal(1, (long)arr[0]);
    }

    [Fact]
    public async Task HExpire_with_LT_only_sets_if_new_expiry_less()
    {
        await _db.HashSetAsync("hexp_lt", "f1", "v1");
        await _db.ExecuteAsync("HEXPIRE", "hexp_lt", "100", "FIELDS", "1", "f1");

        // LT with larger expiry should fail
        var result = await _db.ExecuteAsync("HEXPIRE", "hexp_lt", "200", "LT", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(0, (long)arr[0]);

        // LT with smaller expiry should succeed
        result = await _db.ExecuteAsync("HEXPIRE", "hexp_lt", "50", "LT", "FIELDS", "1", "f1");
        arr = (RedisResult[])result!;
        Assert.Equal(1, (long)arr[0]);
    }

    // ============================================================
    // HPEXPIRE tests
    // Ref: https://redis.io/docs/latest/commands/hpexpire/
    // ============================================================

    [Fact]
    public async Task HPExpire_sets_field_ttl_in_milliseconds()
    {
        await _db.HashSetAsync("hpexp1", "f1", "v1");

        var result = await _db.ExecuteAsync("HPEXPIRE", "hpexp1", "60000", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(1, (long)arr[0]);
    }

    [Fact]
    public async Task HPExpire_field_expires_at_millisecond_precision()
    {
        await _db.HashSetAsync("hpexp_die", "f1", "v1");
        await _db.ExecuteAsync("HPEXPIRE", "hpexp_die", "200", "FIELDS", "1", "f1");

        await Task.Delay(50);
        var val = await _db.HashGetAsync("hpexp_die", "f1");
        Assert.False(val.IsNull);

        await Task.Delay(300);
        val = await _db.HashGetAsync("hpexp_die", "f1");
        Assert.True(val.IsNull);
    }

    // ============================================================
    // HEXPIREAT tests
    // Ref: https://redis.io/docs/latest/commands/hexpireat/
    // ============================================================

    [Fact]
    public async Task HExpireAt_sets_absolute_unix_timestamp_seconds()
    {
        await _db.HashSetAsync("hexpat1", "f1", "v1");
        var futureTs = DateTimeOffset.UtcNow.AddSeconds(60).ToUnixTimeSeconds();

        var result = await _db.ExecuteAsync("HEXPIREAT", "hexpat1", futureTs.ToString(), "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(1, (long)arr[0]);
    }

    [Fact]
    public async Task HExpireAt_past_timestamp_deletes_field()
    {
        await _db.HashSetAsync("hexpat_past", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        var pastTs = DateTimeOffset.UtcNow.AddSeconds(-10).ToUnixTimeSeconds();

        var result = await _db.ExecuteAsync("HEXPIREAT", "hexpat_past", pastTs.ToString(), "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(2, (long)arr[0]); // 2 = deleted due to past expiry

        // f1 should be gone
        var val = await _db.HashGetAsync("hexpat_past", "f1");
        Assert.True(val.IsNull);
        // f2 should still exist
        val = await _db.HashGetAsync("hexpat_past", "f2");
        Assert.Equal("v2", val.ToString());
    }

    // ============================================================
    // HPEXPIREAT tests
    // Ref: https://redis.io/docs/latest/commands/hpexpireat/
    // ============================================================

    [Fact]
    public async Task HPExpireAt_sets_absolute_unix_timestamp_milliseconds()
    {
        await _db.HashSetAsync("hpexpat1", "f1", "v1");
        var futureMs = DateTimeOffset.UtcNow.AddSeconds(60).ToUnixTimeMilliseconds();

        var result = await _db.ExecuteAsync("HPEXPIREAT", "hpexpat1", futureMs.ToString(), "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(1, (long)arr[0]);
    }

    // ============================================================
    // HTTL tests
    // Ref: https://redis.io/docs/latest/commands/httl/
    // ============================================================

    [Fact]
    public async Task HTtl_returns_ttl_in_seconds()
    {
        await _db.HashSetAsync("httl1", "f1", "v1");
        await _db.ExecuteAsync("HEXPIRE", "httl1", "60", "FIELDS", "1", "f1");

        var result = await _db.ExecuteAsync("HTTL", "httl1", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        var ttl = (long)arr[0];
        Assert.InRange(ttl, 50, 60);
    }

    [Fact]
    public async Task HTtl_returns_negative1_for_no_expiry()
    {
        await _db.HashSetAsync("httl2", "f1", "v1");

        var result = await _db.ExecuteAsync("HTTL", "httl2", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(-1, (long)arr[0]);
    }

    [Fact]
    public async Task HTtl_returns_negative2_for_missing_field()
    {
        await _db.HashSetAsync("httl3", "f1", "v1");

        var result = await _db.ExecuteAsync("HTTL", "httl3", "FIELDS", "1", "missing");
        var arr = (RedisResult[])result!;
        Assert.Equal(-2, (long)arr[0]);
    }

    [Fact]
    public async Task HTtl_returns_negative2_for_missing_key()
    {
        var result = await _db.ExecuteAsync("HTTL", "httl_nokey", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(-2, (long)arr[0]);
    }

    // ============================================================
    // HPTTL tests
    // Ref: https://redis.io/docs/latest/commands/hpttl/
    // ============================================================

    [Fact]
    public async Task HPTtl_returns_ttl_in_milliseconds()
    {
        await _db.HashSetAsync("hpttl1", "f1", "v1");
        await _db.ExecuteAsync("HPEXPIRE", "hpttl1", "60000", "FIELDS", "1", "f1");

        var result = await _db.ExecuteAsync("HPTTL", "hpttl1", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        var pttl = (long)arr[0];
        Assert.InRange(pttl, 50000, 60000);
    }

    [Fact]
    public async Task HPTtl_returns_negative1_for_no_expiry()
    {
        await _db.HashSetAsync("hpttl2", "f1", "v1");

        var result = await _db.ExecuteAsync("HPTTL", "hpttl2", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(-1, (long)arr[0]);
    }

    // ============================================================
    // HEXPIRETIME tests
    // Ref: https://redis.io/docs/latest/commands/hexpiretime/
    // ============================================================

    [Fact]
    public async Task HExpireTime_returns_absolute_unix_timestamp()
    {
        await _db.HashSetAsync("hexptime1", "f1", "v1");
        var futureTs = DateTimeOffset.UtcNow.AddSeconds(60).ToUnixTimeSeconds();
        await _db.ExecuteAsync("HEXPIREAT", "hexptime1", futureTs.ToString(), "FIELDS", "1", "f1");

        var result = await _db.ExecuteAsync("HEXPIRETIME", "hexptime1", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        var ts = (long)arr[0];
        // Should be close to what we set (within a small margin)
        Assert.InRange(ts, futureTs - 2, futureTs + 2);
    }

    [Fact]
    public async Task HExpireTime_returns_negative1_for_no_expiry()
    {
        await _db.HashSetAsync("hexptime2", "f1", "v1");

        var result = await _db.ExecuteAsync("HEXPIRETIME", "hexptime2", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(-1, (long)arr[0]);
    }

    [Fact]
    public async Task HExpireTime_returns_negative2_for_missing_field()
    {
        await _db.HashSetAsync("hexptime3", "f1", "v1");

        var result = await _db.ExecuteAsync("HEXPIRETIME", "hexptime3", "FIELDS", "1", "missing");
        var arr = (RedisResult[])result!;
        Assert.Equal(-2, (long)arr[0]);
    }

    // ============================================================
    // HPEXPIRETIME tests
    // Ref: https://redis.io/docs/latest/commands/hpexpiretime/
    // ============================================================

    [Fact]
    public async Task HPExpireTime_returns_absolute_unix_timestamp_millis()
    {
        await _db.HashSetAsync("hpexptime1", "f1", "v1");
        var futureMs = DateTimeOffset.UtcNow.AddSeconds(60).ToUnixTimeMilliseconds();
        await _db.ExecuteAsync("HPEXPIREAT", "hpexptime1", futureMs.ToString(), "FIELDS", "1", "f1");

        var result = await _db.ExecuteAsync("HPEXPIRETIME", "hpexptime1", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        var ts = (long)arr[0];
        Assert.InRange(ts, futureMs - 2000, futureMs + 2000);
    }

    // ============================================================
    // HPERSIST tests
    // Ref: https://redis.io/docs/latest/commands/hpersist/
    // ============================================================

    [Fact]
    public async Task HPersist_removes_field_expiry()
    {
        await _db.HashSetAsync("hpers1", "f1", "v1");
        await _db.ExecuteAsync("HEXPIRE", "hpers1", "60", "FIELDS", "1", "f1");

        var result = await _db.ExecuteAsync("HPERSIST", "hpers1", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(1, (long)arr[0]); // 1 = expiry removed

        // Verify TTL is now -1 (no expiry)
        var ttlResult = await _db.ExecuteAsync("HTTL", "hpers1", "FIELDS", "1", "f1");
        var ttlArr = (RedisResult[])ttlResult!;
        Assert.Equal(-1, (long)ttlArr[0]);
    }

    [Fact]
    public async Task HPersist_returns_negative1_for_field_without_expiry()
    {
        await _db.HashSetAsync("hpers2", "f1", "v1");

        var result = await _db.ExecuteAsync("HPERSIST", "hpers2", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(-1, (long)arr[0]);
    }

    [Fact]
    public async Task HPersist_returns_negative2_for_missing_field()
    {
        await _db.HashSetAsync("hpers3", "f1", "v1");

        var result = await _db.ExecuteAsync("HPERSIST", "hpers3", "FIELDS", "1", "missing");
        var arr = (RedisResult[])result!;
        Assert.Equal(-2, (long)arr[0]);
    }

    [Fact]
    public async Task HPersist_returns_negative2_for_missing_key()
    {
        var result = await _db.ExecuteAsync("HPERSIST", "hpers_nokey", "FIELDS", "1", "f1");
        var arr = (RedisResult[])result!;
        Assert.Equal(-2, (long)arr[0]);
    }


    // ============================================================
    // Integration with existing commands (expired fields)
    // ============================================================

    [Fact]
    public async Task HGetAll_excludes_expired_fields()
    {
        await _db.HashSetAsync("hga_exp", new HashEntry[] { new("f1", "v1"), new("f2", "v2"), new("f3", "v3") });
        await _db.ExecuteAsync("HPEXPIRE", "hga_exp", "100", "FIELDS", "1", "f1");

        await Task.Delay(200);

        var all = await _db.HashGetAllAsync("hga_exp");
        Assert.Equal(2, all.Length);
        Assert.DoesNotContain(all, e => e.Name == "f1");
    }

    [Fact]
    public async Task HKeys_excludes_expired_fields()
    {
        await _db.HashSetAsync("hk_exp", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        await _db.ExecuteAsync("HPEXPIRE", "hk_exp", "100", "FIELDS", "1", "f1");

        await Task.Delay(200);

        var keys = await _db.ExecuteAsync("HKEYS", "hk_exp");
        var arr = (RedisResult[])keys!;
        Assert.Single(arr);
        Assert.Equal("f2", arr[0].ToString());
    }

    [Fact]
    public async Task HVals_excludes_expired_fields()
    {
        await _db.HashSetAsync("hv_exp", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        await _db.ExecuteAsync("HPEXPIRE", "hv_exp", "100", "FIELDS", "1", "f1");

        await Task.Delay(200);

        var vals = await _db.ExecuteAsync("HVALS", "hv_exp");
        var arr = (RedisResult[])vals!;
        Assert.Single(arr);
        Assert.Equal("v2", arr[0].ToString());
    }

    [Fact]
    public async Task HLen_excludes_expired_fields()
    {
        await _db.HashSetAsync("hl_exp", new HashEntry[] { new("f1", "v1"), new("f2", "v2"), new("f3", "v3") });
        await _db.ExecuteAsync("HPEXPIRE", "hl_exp", "100", "FIELDS", "2", "f1", "f2");

        await Task.Delay(200);

        var len = await _db.HashLengthAsync("hl_exp");
        Assert.Equal(1, len);
    }

    [Fact]
    public async Task HExists_returns_false_for_expired_field()
    {
        await _db.HashSetAsync("he_exp", "f1", "v1");
        await _db.ExecuteAsync("HPEXPIRE", "he_exp", "100", "FIELDS", "1", "f1");

        await Task.Delay(200);

        Assert.False(await _db.HashExistsAsync("he_exp", "f1"));
    }

    [Fact]
    public async Task HMGet_returns_nil_for_expired_fields()
    {
        await _db.HashSetAsync("hmg_exp", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        await _db.ExecuteAsync("HPEXPIRE", "hmg_exp", "100", "FIELDS", "1", "f1");

        await Task.Delay(200);

        var results = await _db.HashGetAsync("hmg_exp", new RedisValue[] { "f1", "f2" });
        Assert.True(results[0].IsNull);
        Assert.Equal("v2", results[1].ToString());
    }

    [Fact]
    public async Task HStrLen_returns_zero_for_expired_field()
    {
        await _db.HashSetAsync("hsl_exp", "f1", "value1");
        await _db.ExecuteAsync("HPEXPIRE", "hsl_exp", "100", "FIELDS", "1", "f1");

        await Task.Delay(200);

        var result = await _db.ExecuteAsync("HSTRLEN", "hsl_exp", "f1");
        Assert.Equal(0, (long)result);
    }

    [Fact]
    public async Task HDel_expired_field_returns_zero()
    {
        await _db.HashSetAsync("hd_exp", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        await _db.ExecuteAsync("HPEXPIRE", "hd_exp", "100", "FIELDS", "1", "f1");

        await Task.Delay(200);

        var deleted = await _db.HashDeleteAsync("hd_exp", "f1");
        Assert.False(deleted);
    }

    // ============================================================
    // Edge cases
    // ============================================================

    [Fact]
    public async Task HSet_on_field_with_expiry_clears_expiry()
    {
        // Ref: https://redis.io/docs/latest/develop/data-types/hashes/#field-expiration
        //   "If a field with an active TTL is overwritten by HSET or equivalent commands
        //    (like HMSET or HSETEX without options), the existing TTL is removed and
        //    the field becomes persistent again."
        await _db.HashSetAsync("hset_pres", "f1", "v1");
        await _db.ExecuteAsync("HEXPIRE", "hset_pres", "300", "FIELDS", "1", "f1");

        // Overwrite value with HSET
        await _db.HashSetAsync("hset_pres", "f1", "v2");

        // Verify value updated
        Assert.Equal("v2", (await _db.HashGetAsync("hset_pres", "f1")).ToString());

        // Verify expiry is cleared (-1 = no expiry)
        var ttlResult = await _db.ExecuteAsync("HTTL", "hset_pres", "FIELDS", "1", "f1");
        var ttlArr = (RedisResult[])ttlResult!;
        Assert.Equal(-1, (long)ttlArr[0]);
    }

    [Fact]
    public async Task HScan_excludes_expired_fields()
    {
        await _db.HashSetAsync("hscan_exp", new HashEntry[] { new("f1", "v1"), new("f2", "v2"), new("f3", "v3") });
        await _db.ExecuteAsync("HPEXPIRE", "hscan_exp", "100", "FIELDS", "1", "f2");

        await Task.Delay(200);

        // Scan all fields
        var result = await _db.ExecuteAsync("HSCAN", "hscan_exp", "0");
        var outer = (RedisResult[])result!;
        var fields = (RedisResult[])outer[1]!;
        // fields should contain f1/v1 and f3/v3 (4 entries), not f2
        var fieldNames = new List<string>();
        for (int i = 0; i < fields.Length; i += 2)
            fieldNames.Add(fields[i].ToString()!);
        Assert.DoesNotContain("f2", fieldNames);
        Assert.Contains("f1", fieldNames);
        Assert.Contains("f3", fieldNames);
    }
}
