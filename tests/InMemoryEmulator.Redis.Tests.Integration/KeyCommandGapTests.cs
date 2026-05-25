using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class KeyCommandGapTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public KeyCommandGapTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ═══════════════════════════════════════════════════════════════
    // UNLINK tests
    // Ref: https://redis.io/docs/latest/commands/unlink/
    //   "This command is very similar to DEL: it removes the specified keys.
    //    Just like DEL a key is ignored if it does not exist."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Unlink_removes_existing_key()
    {
        await _db.StringSetAsync("unlink_key1", "value");
        var removed = await _db.KeyDeleteAsync("unlink_key1");
        Assert.True(removed);
        Assert.False(await _db.KeyExistsAsync("unlink_key1"));
    }

    [Fact]
    public async Task Unlink_nonexistent_key_returns_false()
    {
        var removed = await _db.KeyDeleteAsync("unlink_nonexist");
        Assert.False(removed);
    }

    [Fact]
    public async Task Unlink_multiple_keys_returns_count()
    {
        await _db.StringSetAsync("unlink_m1", "v");
        await _db.StringSetAsync("unlink_m2", "v");
        var deleted = await _db.KeyDeleteAsync(new RedisKey[] { "unlink_m1", "unlink_m2", "unlink_m3" });
        Assert.Equal(2, deleted);
    }

    // ═══════════════════════════════════════════════════════════════
    // PEXPIRE tests
    // Ref: https://redis.io/docs/latest/commands/pexpire/
    //   "This command works exactly like EXPIRE but the time to live of
    //    the key is specified in milliseconds instead of seconds."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task PExpire_sets_millisecond_expiry()
    {
        await _db.StringSetAsync("pexpire_key", "value");
        var result = await _db.KeyExpireAsync("pexpire_key", TimeSpan.FromMilliseconds(5000));
        Assert.True(result);
        var ttl = await _db.KeyTimeToLiveAsync("pexpire_key");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalMilliseconds > 3000 && ttl.Value.TotalMilliseconds <= 5000);
    }

    [Fact]
    public async Task PExpire_key_expires_after_short_ms_delay()
    {
        await _db.StringSetAsync("pexpire_short", "value");
        await _db.KeyExpireAsync("pexpire_short", TimeSpan.FromMilliseconds(100));
        Assert.True(await _db.KeyExistsAsync("pexpire_short"));
        await Task.Delay(200);
        Assert.False(await _db.KeyExistsAsync("pexpire_short"));
    }

    // ═══════════════════════════════════════════════════════════════
    // EXPIREAT tests
    // Ref: https://redis.io/docs/latest/commands/expireat/
    //   "EXPIREAT has the same effect and semantic as EXPIRE, but instead
    //    of specifying the number of seconds for the TTL, it takes an
    //    absolute Unix timestamp (seconds since January 1, 1970)."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task ExpireAt_sets_absolute_timestamp_expiry()
    {
        await _db.StringSetAsync("expireat_key", "value");
        var expireTime = DateTime.UtcNow.AddSeconds(100);
        var result = await _db.KeyExpireAsync("expireat_key", expireTime);
        Assert.True(result);
        var ttl = await _db.KeyTimeToLiveAsync("expireat_key");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds > 50 && ttl.Value.TotalSeconds <= 100);
    }

    [Fact]
    public async Task ExpireAt_nonexistent_key_returns_false()
    {
        var result = await _db.KeyExpireAsync("expireat_missing", DateTime.UtcNow.AddSeconds(100));
        Assert.False(result);
    }

    // ═══════════════════════════════════════════════════════════════
    // PEXPIREAT tests
    // Ref: https://redis.io/docs/latest/commands/pexpireat/
    //   "PEXPIREAT has the same effect and semantic as EXPIREAT, but
    //    the Unix time at which the key will expire is specified in
    //    milliseconds instead of seconds."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task PExpireAt_sets_absolute_ms_timestamp_expiry()
    {
        await _db.StringSetAsync("pexpireat_key", "value");
        var msTimestamp = DateTimeOffset.UtcNow.AddSeconds(100).ToUnixTimeMilliseconds();
        var result = await _db.ExecuteAsync("PEXPIREAT", "pexpireat_key", msTimestamp.ToString());
        Assert.Equal(1, (long)result);
        var ttl = await _db.KeyTimeToLiveAsync("pexpireat_key");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds > 50 && ttl.Value.TotalSeconds <= 100);
    }

    [Fact]
    public async Task PExpireAt_nonexistent_key_returns_zero()
    {
        var msTimestamp = DateTimeOffset.UtcNow.AddSeconds(100).ToUnixTimeMilliseconds();
        var result = await _db.ExecuteAsync("PEXPIREAT", "pexpireat_missing", msTimestamp.ToString());
        Assert.Equal(0, (long)result);
    }

    // ═══════════════════════════════════════════════════════════════
    // EXPIRETIME tests
    // Ref: https://redis.io/docs/latest/commands/expiretime/
    //   "Returns the absolute Unix timestamp (since January 1, 1970)
    //    in seconds at which the given key will expire."
    //   "Returns -1 if the key exists but has no associated expiry."
    //   "Returns -2 if the key does not exist."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task ExpireTime_returns_unix_timestamp_when_expiry_set()
    {
        await _db.StringSetAsync("expiretime_key", "value");
        var futureTime = DateTimeOffset.UtcNow.AddSeconds(100);
        await _db.KeyExpireAsync("expiretime_key", futureTime.UtcDateTime);
        var result = await _db.ExecuteAsync("EXPIRETIME", "expiretime_key");
        var timestamp = (long)result;
        // Should be close to the future timestamp (within a few seconds tolerance)
        Assert.InRange(timestamp, futureTime.ToUnixTimeSeconds() - 2, futureTime.ToUnixTimeSeconds() + 2);
    }

    [Fact]
    public async Task ExpireTime_returns_minus_one_when_no_expiry()
    {
        await _db.StringSetAsync("expiretime_noexp", "value");
        var result = await _db.ExecuteAsync("EXPIRETIME", "expiretime_noexp");
        Assert.Equal(-1, (long)result);
    }

    [Fact]
    public async Task ExpireTime_returns_minus_two_when_key_missing()
    {
        var result = await _db.ExecuteAsync("EXPIRETIME", "expiretime_missing");
        Assert.Equal(-2, (long)result);
    }

    // ═══════════════════════════════════════════════════════════════
    // PEXPIRETIME tests
    // Ref: https://redis.io/docs/latest/commands/pexpiretime/
    //   "Like EXPIRETIME but returns the absolute Unix expiration
    //    timestamp in milliseconds instead of seconds."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task PExpireTime_returns_unix_ms_timestamp_when_expiry_set()
    {
        await _db.StringSetAsync("pexpiretime_key", "value");
        var futureTime = DateTimeOffset.UtcNow.AddSeconds(100);
        await _db.KeyExpireAsync("pexpiretime_key", futureTime.UtcDateTime);
        var result = await _db.ExecuteAsync("PEXPIRETIME", "pexpiretime_key");
        var msTimestamp = (long)result;
        var expectedMs = futureTime.ToUnixTimeMilliseconds();
        // Allow a few seconds tolerance
        Assert.InRange(msTimestamp, expectedMs - 2000, expectedMs + 2000);
    }

    [Fact]
    public async Task PExpireTime_returns_minus_one_when_no_expiry()
    {
        await _db.StringSetAsync("pexpiretime_noexp", "value");
        var result = await _db.ExecuteAsync("PEXPIRETIME", "pexpiretime_noexp");
        Assert.Equal(-1, (long)result);
    }

    [Fact]
    public async Task PExpireTime_returns_minus_two_when_key_missing()
    {
        var result = await _db.ExecuteAsync("PEXPIRETIME", "pexpiretime_missing");
        Assert.Equal(-2, (long)result);
    }

    // ═══════════════════════════════════════════════════════════════
    // RENAMENX tests
    // Ref: https://redis.io/docs/latest/commands/renamenx/
    //   "Renames key to newkey if newkey does not yet exist.
    //    It returns an error when key does not exist."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task RenameNx_succeeds_when_dest_does_not_exist()
    {
        await _db.StringSetAsync("renamenx_src", "value");
        var result = await _db.KeyRenameAsync("renamenx_src", "renamenx_dst", When.NotExists);
        Assert.True(result);
        Assert.False(await _db.KeyExistsAsync("renamenx_src"));
        Assert.Equal("value", (await _db.StringGetAsync("renamenx_dst")).ToString());
    }

    [Fact]
    public async Task RenameNx_fails_when_dest_exists()
    {
        await _db.StringSetAsync("renamenx_src2", "src_val");
        await _db.StringSetAsync("renamenx_dst2", "dst_val");
        var result = await _db.KeyRenameAsync("renamenx_src2", "renamenx_dst2", When.NotExists);
        Assert.False(result);
        // Source should still exist
        Assert.Equal("src_val", (await _db.StringGetAsync("renamenx_src2")).ToString());
        // Dest should still have original value
        Assert.Equal("dst_val", (await _db.StringGetAsync("renamenx_dst2")).ToString());
    }

    [Fact]
    public async Task RenameNx_nonexistent_source_throws()
    {
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.KeyRenameAsync("renamenx_ghost", "renamenx_any", When.NotExists));
        Assert.Contains("no such key", ex.Message);
    }

    // ═══════════════════════════════════════════════════════════════
    // RANDOMKEY tests
    // Ref: https://redis.io/docs/latest/commands/randomkey/
    //   "Return a random key from the currently selected database."
    //   "Returns nil when the database is empty."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task RandomKey_returns_existing_key()
    {
        await _db.StringSetAsync("randomkey_a", "v");
        await _db.StringSetAsync("randomkey_b", "v");
        await _db.StringSetAsync("randomkey_c", "v");
        var key = await _db.KeyRandomAsync();
        Assert.False(((string?)key) == null);
        Assert.True(await _db.KeyExistsAsync(key));
    }

    [Fact]
    public async Task RandomKey_returns_null_on_empty_db()
    {
        // Database was flushed in InitializeAsync, so it should be empty
        var key = await _db.KeyRandomAsync();
        Assert.True(((string?)key) == null);
    }

    // ═══════════════════════════════════════════════════════════════
    // TOUCH tests
    // Ref: https://redis.io/docs/latest/commands/touch/
    //   "Returns the number of keys that exist. Keys mentioned multiple
    //    times in the argument list are counted multiple times."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Touch_returns_one_for_existing_key()
    {
        await _db.StringSetAsync("touch_key", "value");
        var result = await _db.KeyTouchAsync("touch_key");
        Assert.True(result);
    }

    [Fact]
    public async Task Touch_returns_false_for_nonexistent_key()
    {
        var result = await _db.KeyTouchAsync("touch_missing");
        Assert.False(result);
    }

    [Fact]
    public async Task Touch_multiple_keys_returns_count()
    {
        await _db.StringSetAsync("touch_m1", "v");
        await _db.StringSetAsync("touch_m2", "v");
        var result = await _db.KeyTouchAsync(new RedisKey[] { "touch_m1", "touch_m2", "touch_m3" });
        Assert.Equal(2, result);
    }

    // ═══════════════════════════════════════════════════════════════
    // OBJECT ENCODING tests
    // Ref: https://redis.io/docs/latest/commands/object-encoding/
    //   "Returns the internal encoding for the Redis object stored at <key>."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Object_Encoding_returns_encoding_for_string()
    {
        await _db.StringSetAsync("objenc_str", "hello");
        var result = await _db.ExecuteAsync("OBJECT", "ENCODING", "objenc_str");
        var encoding = (string)result!;
        // Redis returns "embstr", "int", or "raw" for strings
        Assert.Contains(encoding, new[] { "embstr", "int", "raw" });
    }

    [Fact]
    public async Task Object_Encoding_returns_int_for_numeric_string()
    {
        await _db.StringSetAsync("objenc_int", "12345");
        var result = await _db.ExecuteAsync("OBJECT", "ENCODING", "objenc_int");
        Assert.Equal("int", (string)result!);
    }

    [Fact]
    public async Task Object_Encoding_returns_encoding_for_list()
    {
        await _db.ListRightPushAsync("objenc_list", new RedisValue[] { "a", "b", "c" });
        var result = await _db.ExecuteAsync("OBJECT", "ENCODING", "objenc_list");
        var encoding = (string)result!;
        // Redis returns "listpack" or "quicklist" for lists
        Assert.Contains(encoding, new[] { "listpack", "quicklist", "ziplist" });
    }

    [Fact]
    public async Task Object_Encoding_returns_encoding_for_hash()
    {
        await _db.HashSetAsync("objenc_hash", new HashEntry[] { new("f1", "v1") });
        var result = await _db.ExecuteAsync("OBJECT", "ENCODING", "objenc_hash");
        var encoding = (string)result!;
        // Redis returns "listpack" or "hashtable" for hashes
        Assert.Contains(encoding, new[] { "listpack", "hashtable", "ziplist" });
    }

    [Fact]
    public async Task Object_Encoding_returns_encoding_for_set()
    {
        await _db.SetAddAsync("objenc_set", new RedisValue[] { "a", "b", "c" });
        var result = await _db.ExecuteAsync("OBJECT", "ENCODING", "objenc_set");
        var encoding = (string)result!;
        // Redis returns "listpack", "hashtable", or "intset" for sets
        Assert.Contains(encoding, new[] { "listpack", "hashtable", "intset", "ziplist" });
    }

    [Fact]
    public async Task Object_Encoding_returns_encoding_for_zset()
    {
        await _db.SortedSetAddAsync("objenc_zset", new SortedSetEntry[] { new("a", 1), new("b", 2) });
        var result = await _db.ExecuteAsync("OBJECT", "ENCODING", "objenc_zset");
        var encoding = (string)result!;
        // Redis returns "listpack" or "skiplist" for sorted sets
        Assert.Contains(encoding, new[] { "listpack", "skiplist", "ziplist" });
    }

    // ═══════════════════════════════════════════════════════════════
    // OBJECT REFCOUNT tests
    // Ref: https://redis.io/docs/latest/commands/object-refcount/
    //   "Returns the reference count of the object stored at <key>."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Object_RefCount_returns_count_for_existing_key()
    {
        await _db.StringSetAsync("objref_key", "value");
        var result = await _db.ExecuteAsync("OBJECT", "REFCOUNT", "objref_key");
        var refCount = (long)result;
        Assert.True(refCount >= 1);
    }

    [Fact]
    public async Task Object_RefCount_nonexistent_key_returns_null()
    {
        var result = await _db.ExecuteAsync("OBJECT", "REFCOUNT", "objref_missing");
        Assert.True(result.IsNull);
    }

    // ═══════════════════════════════════════════════════════════════
    // WAIT tests
    // Ref: https://redis.io/docs/latest/commands/wait/
    //   "This command blocks the current client until all previous write
    //    commands are successfully transferred and acknowledged by at
    //    least the specified number of replicas."
    //   Returns the number of replicas reached.
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Wait_returns_zero_with_no_replicas()
    {
        var result = await _db.ExecuteAsync("WAIT", "0", "0");
        var replicas = (long)result;
        Assert.Equal(0, replicas);
    }

    // ═══════════════════════════════════════════════════════════════
    // EXPIRE with NX/XX/GT/LT subcommands (Redis 7.0+)
    // Ref: https://redis.io/docs/latest/commands/expire/
    //   "NX -- Set expiry only when the key has no expiry"
    //   "XX -- Set expiry only when the key has an existing expiry"
    //   "GT -- Set expiry only when the new expiry is greater than current one"
    //   "LT -- Set expiry only when the new expiry is less than current one"
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Expire_NX_sets_expiry_when_no_existing_expiry()
    {
        await _db.StringSetAsync("expire_nx_key", "value");
        var result = await _db.ExecuteAsync("EXPIRE", "expire_nx_key", "60", "NX");
        Assert.Equal(1, (long)result);
        var ttl = await _db.KeyTimeToLiveAsync("expire_nx_key");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds > 30);
    }

    [Fact]
    public async Task Expire_NX_does_not_set_when_expiry_already_exists()
    {
        await _db.StringSetAsync("expire_nx_exist", "value", TimeSpan.FromSeconds(30));
        var result = await _db.ExecuteAsync("EXPIRE", "expire_nx_exist", "120", "NX");
        Assert.Equal(0, (long)result);
        // TTL should remain around 30, not 120
        var ttl = await _db.KeyTimeToLiveAsync("expire_nx_exist");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds <= 30);
    }

    [Fact]
    public async Task Expire_XX_sets_expiry_when_expiry_already_exists()
    {
        await _db.StringSetAsync("expire_xx_key", "value", TimeSpan.FromSeconds(30));
        var result = await _db.ExecuteAsync("EXPIRE", "expire_xx_key", "60", "XX");
        Assert.Equal(1, (long)result);
        var ttl = await _db.KeyTimeToLiveAsync("expire_xx_key");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds > 30);
    }

    [Fact]
    public async Task Expire_XX_does_not_set_when_no_existing_expiry()
    {
        await _db.StringSetAsync("expire_xx_noexp", "value");
        var result = await _db.ExecuteAsync("EXPIRE", "expire_xx_noexp", "60", "XX");
        Assert.Equal(0, (long)result);
        var ttl = await _db.KeyTimeToLiveAsync("expire_xx_noexp");
        Assert.Null(ttl);
    }

    [Fact]
    public async Task Expire_GT_sets_expiry_when_new_is_greater()
    {
        await _db.StringSetAsync("expire_gt_key", "value", TimeSpan.FromSeconds(30));
        var result = await _db.ExecuteAsync("EXPIRE", "expire_gt_key", "120", "GT");
        Assert.Equal(1, (long)result);
        var ttl = await _db.KeyTimeToLiveAsync("expire_gt_key");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds > 60);
    }

    [Fact]
    public async Task Expire_GT_does_not_set_when_new_is_less()
    {
        await _db.StringSetAsync("expire_gt_less", "value", TimeSpan.FromSeconds(120));
        var result = await _db.ExecuteAsync("EXPIRE", "expire_gt_less", "30", "GT");
        Assert.Equal(0, (long)result);
        var ttl = await _db.KeyTimeToLiveAsync("expire_gt_less");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds > 60);
    }

    [Fact]
    public async Task Expire_LT_sets_expiry_when_new_is_less()
    {
        await _db.StringSetAsync("expire_lt_key", "value", TimeSpan.FromSeconds(120));
        var result = await _db.ExecuteAsync("EXPIRE", "expire_lt_key", "30", "LT");
        Assert.Equal(1, (long)result);
        var ttl = await _db.KeyTimeToLiveAsync("expire_lt_key");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds <= 30);
    }

    [Fact]
    public async Task Expire_LT_does_not_set_when_new_is_greater()
    {
        await _db.StringSetAsync("expire_lt_greater", "value", TimeSpan.FromSeconds(30));
        var result = await _db.ExecuteAsync("EXPIRE", "expire_lt_greater", "120", "LT");
        Assert.Equal(0, (long)result);
        var ttl = await _db.KeyTimeToLiveAsync("expire_lt_greater");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds <= 30);
    }

    // ═══════════════════════════════════════════════════════════════
    // SORT_RO tests
    // Ref: https://redis.io/docs/latest/commands/sort_ro/
    //   "Read-only variant of the SORT command. It is exactly like the
    //    original SORT command with the STORE option."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task Sort_RO_returns_sorted_numeric_list()
    {
        await _db.ListRightPushAsync("sortro_nums", new RedisValue[] { "3", "1", "2" });
        var result = await _db.ExecuteAsync("SORT_RO", "sortro_nums");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal("1", (string)arr[0]!);
        Assert.Equal("2", (string)arr[1]!);
        Assert.Equal("3", (string)arr[2]!);
    }

    [Fact]
    public async Task Sort_RO_alpha_returns_alphabetical_order()
    {
        await _db.ListRightPushAsync("sortro_alpha", new RedisValue[] { "banana", "apple", "cherry" });
        var result = await _db.ExecuteAsync("SORT_RO", "sortro_alpha", "ALPHA");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal("apple", (string)arr[0]!);
        Assert.Equal("banana", (string)arr[1]!);
        Assert.Equal("cherry", (string)arr[2]!);
    }

    [Fact]
    public async Task Sort_RO_desc_returns_descending_order()
    {
        await _db.ListRightPushAsync("sortro_desc", new RedisValue[] { "1", "3", "2" });
        var result = await _db.ExecuteAsync("SORT_RO", "sortro_desc", "DESC");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal("3", (string)arr[0]!);
        Assert.Equal("2", (string)arr[1]!);
        Assert.Equal("1", (string)arr[2]!);
    }

    [Fact]
    public async Task Sort_RO_nonexistent_key_returns_empty()
    {
        var result = await _db.ExecuteAsync("SORT_RO", "sortro_missing");
        var arr = (RedisResult[])result!;
        Assert.Empty(arr);
    }

    // ═══════════════════════════════════════════════════════════════
    // PTTL tests
    // Ref: https://redis.io/docs/latest/commands/pttl/
    //   "Like TTL this command returns the remaining time to live of a
    //    key that has an expire set, with the sole difference that TTL
    //    returns the amount of remaining time in seconds while PTTL
    //    returns it in milliseconds."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task PTTL_returns_millisecond_precision_ttl()
    {
        await _db.StringSetAsync("pttl_key", "value", TimeSpan.FromSeconds(10));
        var ttl = await _db.KeyTimeToLiveAsync("pttl_key");
        Assert.NotNull(ttl);
        // KeyTimeToLiveAsync sends PTTL and returns TimeSpan with ms precision
        Assert.True(ttl!.Value.TotalMilliseconds > 8000 && ttl.Value.TotalMilliseconds <= 10000);
    }

    [Fact]
    public async Task PTTL_returns_null_for_no_expiry()
    {
        await _db.StringSetAsync("pttl_noexp", "value");
        var ttl = await _db.KeyTimeToLiveAsync("pttl_noexp");
        Assert.Null(ttl);
    }

    [Fact]
    public async Task PTTL_returns_null_for_nonexistent_key()
    {
        var ttl = await _db.KeyTimeToLiveAsync("pttl_missing");
        Assert.Null(ttl);
    }
}
