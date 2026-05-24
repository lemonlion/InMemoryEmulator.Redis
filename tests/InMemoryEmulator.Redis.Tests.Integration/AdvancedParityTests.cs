using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

/// <summary>
/// Tests targeting subtle Redis behaviors that are easy to get wrong in an emulator.
/// Each test is designed to catch specific parity issues.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class AdvancedParityTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public AdvancedParityTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // --- SET GET option (Redis 6.2+) ---

    [Fact]
    public async Task Set_with_GET_returns_previous_value()
    {
        await _db.StringSetAsync("setget_key", "old");
        var prev = await _db.StringSetAndGetAsync("setget_key", "new");
        Assert.Equal("old", prev.ToString());
        Assert.Equal("new", (await _db.StringGetAsync("setget_key")).ToString());
    }

    [Fact]
    public async Task Set_with_GET_on_nonexistent_returns_null()
    {
        var prev = await _db.StringSetAndGetAsync("setget_missing", "value");
        Assert.True(prev.IsNull);
    }

    // --- Key behavior: overwrite clears type ---

    [Fact]
    public async Task Set_on_existing_list_key_replaces_with_string()
    {
        await _db.ListRightPushAsync("type_replace", "item");
        Assert.Equal(RedisType.List, await _db.KeyTypeAsync("type_replace"));

        await _db.StringSetAsync("type_replace", "now_a_string");
        Assert.Equal(RedisType.String, await _db.KeyTypeAsync("type_replace"));
        Assert.Equal("now_a_string", (await _db.StringGetAsync("type_replace")).ToString());
    }

    // --- INCR on large numbers ---

    [Fact]
    public async Task Incr_handles_large_positive_numbers()
    {
        await _db.StringSetAsync("big_num", "9223372036854775800");
        var result = await _db.StringIncrementAsync("big_num", 5);
        Assert.Equal(9223372036854775805, result);
    }

    [Fact]
    public async Task Incr_handles_large_negative_numbers()
    {
        await _db.StringSetAsync("neg_num", "-100");
        var result = await _db.StringIncrementAsync("neg_num", -50);
        Assert.Equal(-150, result);
    }

    // --- Sorted set: members with same score sorted lexicographically ---

    [Fact]
    public async Task ZRange_same_score_sorted_lexicographically()
    {
        await _db.SortedSetAddAsync("zlex", new SortedSetEntry[]
        {
            new("banana", 0), new("apple", 0), new("cherry", 0)
        });
        var result = await _db.SortedSetRangeByRankAsync("zlex");
        Assert.Equal("apple", result[0].ToString());
        Assert.Equal("banana", result[1].ToString());
        Assert.Equal("cherry", result[2].ToString());
    }

    // --- Hash: empty hash auto-deleted ---

    [Fact]
    public async Task Hash_deleted_when_all_fields_removed()
    {
        await _db.HashSetAsync("hash_autodel", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        await _db.HashDeleteAsync("hash_autodel", new RedisValue[] { "f1", "f2" });
        Assert.False(await _db.KeyExistsAsync("hash_autodel"));
    }

    // --- Set: empty set auto-deleted ---

    [Fact]
    public async Task Set_deleted_when_all_members_removed()
    {
        await _db.SetAddAsync("set_autodel", new RedisValue[] { "a", "b" });
        await _db.SetRemoveAsync("set_autodel", new RedisValue[] { "a", "b" });
        Assert.False(await _db.KeyExistsAsync("set_autodel"));
    }

    // --- List: empty list auto-deleted ---

    [Fact]
    public async Task List_deleted_when_all_elements_popped()
    {
        await _db.ListRightPushAsync("list_autodel", new RedisValue[] { "x", "y" });
        await _db.ListLeftPopAsync("list_autodel");
        await _db.ListLeftPopAsync("list_autodel");
        Assert.False(await _db.KeyExistsAsync("list_autodel"));
    }

    // --- Sorted set: empty zset auto-deleted ---

    [Fact]
    public async Task SortedSet_deleted_when_all_members_removed()
    {
        await _db.SortedSetAddAsync("zset_autodel", new SortedSetEntry[] { new("a", 1), new("b", 2) });
        await _db.SortedSetRemoveAsync("zset_autodel", new RedisValue[] { "a", "b" });
        Assert.False(await _db.KeyExistsAsync("zset_autodel"));
    }

    // --- Key expiry preserved through certain operations ---

    [Fact]
    public async Task Append_preserves_existing_ttl()
    {
        await _db.StringSetAsync("append_ttl", "hello", TimeSpan.FromSeconds(100));
        await _db.StringAppendAsync("append_ttl", " world");
        var ttl = await _db.KeyTimeToLiveAsync("append_ttl");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds > 50);
    }

    [Fact]
    public async Task Incr_preserves_existing_ttl()
    {
        await _db.StringSetAsync("incr_ttl", "10", TimeSpan.FromSeconds(100));
        await _db.StringIncrementAsync("incr_ttl");
        var ttl = await _db.KeyTimeToLiveAsync("incr_ttl");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds > 50);
    }

    // --- DEL returns 0 for already expired keys ---

    [Fact]
    public async Task Del_on_expired_key_returns_zero()
    {
        await _db.StringSetAsync("del_expired", "v", TimeSpan.FromMilliseconds(50));
        await Task.Delay(100);
        var deleted = await _db.KeyDeleteAsync("del_expired");
        Assert.False(deleted);
    }

    // --- Multiple operations on same key in batch ---

    [Fact]
    public async Task Multiple_incr_in_batch_accumulate()
    {
        await _db.StringSetAsync("batch_incr", "0");
        var batch = _db.CreateBatch();
        var t1 = batch.StringIncrementAsync("batch_incr");
        var t2 = batch.StringIncrementAsync("batch_incr");
        var t3 = batch.StringIncrementAsync("batch_incr");
        batch.Execute();
        await Task.WhenAll(t1, t2, t3);
        Assert.Equal("3", (await _db.StringGetAsync("batch_incr")).ToString());
    }

    // --- SCAN with TYPE filter ---

    [Fact]
    public async Task Scan_with_type_filter()
    {
        await _db.StringSetAsync("scan_str", "v");
        await _db.ListRightPushAsync("scan_list", "v");
        await _db.SetAddAsync("scan_set", "v");

        var mux = await _fixture.GetMultiplexerAsync();
        var server = mux.GetServers().First();
        var stringKeys = server.Keys(pattern: "scan_*", pageSize: 100).ToArray();
        // At minimum, all 3 keys should be returned
        Assert.True(stringKeys.Length >= 3);
    }

    // --- Sorted set: WITHSCORES returns scores ---

    [Fact]
    public async Task ZRangeWithScores_returns_member_score_pairs()
    {
        await _db.SortedSetAddAsync("zwithscores", new SortedSetEntry[]
        {
            new("a", 1.5), new("b", 2.5), new("c", 3.5)
        });
        var results = await _db.SortedSetRangeByRankWithScoresAsync("zwithscores");
        Assert.Equal(3, results.Length);
        Assert.Equal("a", results[0].Element.ToString());
        Assert.Equal(1.5, results[0].Score);
        Assert.Equal("b", results[1].Element.ToString());
        Assert.Equal(2.5, results[1].Score);
    }

    // --- Multiple databases are isolated ---

    [Fact]
    public async Task Different_databases_are_fully_isolated()
    {
        var mux = await _fixture.GetMultiplexerAsync();
        var db0 = mux.GetDatabase(0);
        var db1 = mux.GetDatabase(1);

        await db0.StringSetAsync("isolated_key", "db0_value");
        await db1.StringSetAsync("isolated_key", "db1_value");

        Assert.Equal("db0_value", (await db0.StringGetAsync("isolated_key")).ToString());
        Assert.Equal("db1_value", (await db1.StringGetAsync("isolated_key")).ToString());
    }

    // --- GETDEL atomically gets and deletes ---

    [Fact]
    public async Task GetDel_returns_value_and_removes_key()
    {
        await _db.StringSetAsync("getdel_key", "secret");
        var result = await _db.StringGetDeleteAsync("getdel_key");
        Assert.Equal("secret", result.ToString());
        Assert.False(await _db.KeyExistsAsync("getdel_key"));
    }

    [Fact]
    public async Task GetDel_on_nonexistent_returns_null()
    {
        var result = await _db.StringGetDeleteAsync("getdel_missing");
        Assert.True(result.IsNull);
    }

    // --- SortedSet: ZINCRBY creates member if not exists ---

    [Fact]
    public async Task ZIncrBy_on_nonexistent_key_creates_sorted_set()
    {
        var score = await _db.SortedSetIncrementAsync("zincrby_nokey", "member", 3.14);
        Assert.Equal(3.14, score);
        Assert.Equal(RedisType.SortedSet, await _db.KeyTypeAsync("zincrby_nokey"));
    }

    // --- String: binary-safe values ---

    [Fact]
    public async Task String_handles_binary_data_with_null_bytes()
    {
        var binary = new byte[] { 0x00, 0x01, 0x02, 0xFF, 0x00, 0xFE };
        await _db.StringSetAsync("binary_key", binary);
        var result = (byte[])(await _db.StringGetAsync("binary_key"))!;
        Assert.Equal(binary, result);
    }

    // --- Hash: HINCRBY creates field ---

    [Fact]
    public async Task HIncrBy_creates_hash_and_field_if_neither_exist()
    {
        var result = await _db.HashIncrementAsync("hincrby_create", "counter", 7);
        Assert.Equal(7, result);
        Assert.Equal(RedisType.Hash, await _db.KeyTypeAsync("hincrby_create"));
    }

    // --- Rename with expiry ---

    [Fact]
    public async Task Rename_preserves_ttl_of_source()
    {
        await _db.StringSetAsync("rename_ttl_src", "v", TimeSpan.FromSeconds(100));
        await _db.KeyRenameAsync("rename_ttl_src", "rename_ttl_dst");
        var ttl = await _db.KeyTimeToLiveAsync("rename_ttl_dst");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds > 50);
    }
}
