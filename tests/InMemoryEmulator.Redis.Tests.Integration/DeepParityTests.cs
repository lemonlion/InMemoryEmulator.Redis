using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

/// <summary>
/// Deep parity tests targeting subtle behaviors that diverge between emulators and real Redis.
/// </summary>
[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class DeepParityTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public DeepParityTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // --- GETEX with PERSIST clears TTL ---

    [Fact]
    public async Task GetEx_persist_removes_expiry()
    {
        await _db.StringSetAsync("getex_persist", "v", TimeSpan.FromSeconds(100));
        await _db.ExecuteAsync("GETEX", "getex_persist", "PERSIST");
        var ttl = await _db.KeyTimeToLiveAsync("getex_persist");
        Assert.Null(ttl);
    }

    // --- INCR on empty string equivalent to 0 ---

    [Fact]
    public async Task Incr_on_empty_string_fails()
    {
        await _db.StringSetAsync("incr_empty", "");
        await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.StringIncrementAsync("incr_empty"));
    }

    // --- SET overwrites different type ---

    [Fact]
    public async Task Set_overwrites_hash_with_string()
    {
        await _db.HashSetAsync("overwrite_h", "f", "v");
        await _db.StringSetAsync("overwrite_h", "now_string");
        Assert.Equal(RedisType.String, await _db.KeyTypeAsync("overwrite_h"));
    }

    [Fact]
    public async Task Set_overwrites_sorted_set_with_string()
    {
        await _db.SortedSetAddAsync("overwrite_z", "m", 1.0);
        await _db.StringSetAsync("overwrite_z", "now_string");
        Assert.Equal(RedisType.String, await _db.KeyTypeAsync("overwrite_z"));
    }

    // --- Key with spaces and special characters ---

    [Fact]
    public async Task Key_with_spaces_works()
    {
        await _db.StringSetAsync("key with spaces", "value");
        Assert.Equal("value", (await _db.StringGetAsync("key with spaces")).ToString());
    }

    [Fact]
    public async Task Key_with_colons_works()
    {
        await _db.StringSetAsync("user:123:profile:name", "Alice");
        Assert.Equal("Alice", (await _db.StringGetAsync("user:123:profile:name")).ToString());
    }

    [Fact]
    public async Task Key_with_unicode_works()
    {
        await _db.StringSetAsync("clé_unicode_日本語", "value");
        Assert.Equal("value", (await _db.StringGetAsync("clé_unicode_日本語")).ToString());
    }

    // --- ZADD with identical scores: lexicographic ordering ---

    [Fact]
    public async Task ZRangeByLex_on_same_score_elements()
    {
        await _db.SortedSetAddAsync("zlex_test", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });
        var result = await _db.SortedSetRangeByValueAsync("zlex_test", "b", "d");
        Assert.Equal(3, result.Length);
        Assert.Equal("b", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
        Assert.Equal("d", result[2].ToString());
    }

    // --- RPOPLPUSH same source and dest ---

    [Fact]
    public async Task RPopLPush_same_key_rotates()
    {
        await _db.ListRightPushAsync("rotate", new RedisValue[] { "a", "b", "c" });
        await _db.ListRightPopLeftPushAsync("rotate", "rotate");
        var items = await _db.ListRangeAsync("rotate");
        Assert.Equal("c", items[0].ToString());
        Assert.Equal("a", items[1].ToString());
        Assert.Equal("b", items[2].ToString());
    }

    // --- EXISTS returns count including duplicates ---

    [Fact]
    public async Task Exists_counts_same_key_multiple_times()
    {
        await _db.StringSetAsync("exists_dup", "v");
        var count = (long)await _db.ExecuteAsync("EXISTS", "exists_dup", "exists_dup");
        Assert.Equal(2, count);
    }

    // --- SINTERSTORE with empty result deletes dest ---

    [Fact]
    public async Task SInterStore_empty_result_removes_destination()
    {
        await _db.SetAddAsync("sinter_dst_pre", "pre_existing");
        await _db.SetAddAsync("sinter_s1", new RedisValue[] { "a", "b" });
        await _db.SetAddAsync("sinter_s2", new RedisValue[] { "c", "d" });

        await _db.SetCombineAndStoreAsync(SetOperation.Intersect, "sinter_dst_pre", "sinter_s1", "sinter_s2");
        Assert.False(await _db.KeyExistsAsync("sinter_dst_pre"));
    }

    // --- ZUNIONSTORE default aggregate is SUM ---

    [Fact]
    public async Task ZUnionStore_sums_scores_by_default()
    {
        await _db.SortedSetAddAsync("zu_a", "member", 10);
        await _db.SortedSetAddAsync("zu_b", "member", 20);

        await _db.SortedSetCombineAndStoreAsync(SetOperation.Union, "zu_dst", new RedisKey[] { "zu_a", "zu_b" });
        var score = await _db.SortedSetScoreAsync("zu_dst", "member");
        Assert.Equal(30, score);
    }

    // --- HMGET returns correct order matching request ---

    [Fact]
    public async Task HMGet_returns_in_request_order()
    {
        await _db.HashSetAsync("hmget_order", new HashEntry[]
        {
            new("z_field", "z_val"), new("a_field", "a_val"), new("m_field", "m_val")
        });
        var results = await _db.HashGetAsync("hmget_order", new RedisValue[] { "m_field", "z_field", "a_field" });
        Assert.Equal("m_val", results[0].ToString());
        Assert.Equal("z_val", results[1].ToString());
        Assert.Equal("a_val", results[2].ToString());
    }

    // --- EXPIRE on already expired key returns 0 ---

    [Fact]
    public async Task Expire_on_expired_key_returns_false()
    {
        await _db.StringSetAsync("expire_gone", "v", TimeSpan.FromMilliseconds(50));
        await Task.Delay(100);
        var result = await _db.KeyExpireAsync("expire_gone", TimeSpan.FromSeconds(100));
        Assert.False(result);
    }

    // --- SETRANGE with offset 0 overwrites beginning ---

    [Fact]
    public async Task SetRange_offset_zero_overwrites_start()
    {
        await _db.StringSetAsync("setrange_0", "Hello World");
        await _db.StringSetRangeAsync("setrange_0", 0, "Bye");
        var result = await _db.StringGetAsync("setrange_0");
        Assert.Equal("Byelo World", result.ToString());
    }

    // --- LPUSH multiple values: last arg is at head ---

    [Fact]
    public async Task LPush_multiple_values_order()
    {
        await _db.ListLeftPushAsync("lpush_order", new RedisValue[] { "a", "b", "c" });
        var items = await _db.ListRangeAsync("lpush_order");
        // Redis LPUSH a b c → list is [c, b, a]
        Assert.Equal("c", items[0].ToString());
        Assert.Equal("b", items[1].ToString());
        Assert.Equal("a", items[2].ToString());
    }

    // --- Transaction conditional: StringEqual ---

    [Fact]
    public async Task Transaction_condition_string_equal()
    {
        await _db.StringSetAsync("tx_cond_eq", "expected");
        var tran = _db.CreateTransaction();
        tran.AddCondition(Condition.StringEqual("tx_cond_eq", "expected"));
        _ = tran.StringSetAsync("tx_cond_eq", "new_value");
        var committed = await tran.ExecuteAsync();
        Assert.True(committed);
        Assert.Equal("new_value", (await _db.StringGetAsync("tx_cond_eq")).ToString());
    }

    [Fact]
    public async Task Transaction_condition_string_equal_fails()
    {
        await _db.StringSetAsync("tx_cond_eq2", "actual");
        var tran = _db.CreateTransaction();
        tran.AddCondition(Condition.StringEqual("tx_cond_eq2", "expected"));
        _ = tran.StringSetAsync("tx_cond_eq2", "should_not_set");
        var committed = await tran.ExecuteAsync();
        Assert.False(committed);
        Assert.Equal("actual", (await _db.StringGetAsync("tx_cond_eq2")).ToString());
    }

    // --- SORT numeric ---

    [Fact]
    public async Task Sort_list_numerically()
    {
        await _db.ListRightPushAsync("sort_nums", new RedisValue[] { "10", "2", "30", "1", "25" });
        var sorted = await _db.SortAsync("sort_nums");
        Assert.Equal("1", sorted[0].ToString());
        Assert.Equal("2", sorted[1].ToString());
        Assert.Equal("10", sorted[2].ToString());
        Assert.Equal("25", sorted[3].ToString());
        Assert.Equal("30", sorted[4].ToString());
    }

    [Fact]
    public async Task Sort_list_alphabetically()
    {
        await _db.ListRightPushAsync("sort_alpha", new RedisValue[] { "banana", "apple", "cherry" });
        var sorted = await _db.SortAsync("sort_alpha", sortType: SortType.Alphabetic);
        Assert.Equal("apple", sorted[0].ToString());
        Assert.Equal("banana", sorted[1].ToString());
        Assert.Equal("cherry", sorted[2].ToString());
    }

    [Fact]
    public async Task Sort_descending()
    {
        await _db.ListRightPushAsync("sort_desc", new RedisValue[] { "1", "3", "2" });
        var sorted = await _db.SortAsync("sort_desc", order: Order.Descending);
        Assert.Equal("3", sorted[0].ToString());
        Assert.Equal("2", sorted[1].ToString());
        Assert.Equal("1", sorted[2].ToString());
    }

    // --- HSET returns number of NEW fields added ---

    [Fact]
    public async Task HSet_multiple_returns_new_field_count()
    {
        await _db.HashSetAsync("hset_ret", new HashEntry[] { new("f1", "v1") });
        // Now set f1 (existing) and f2 (new) — Redis HSET returns number of new fields
        var result = await _db.ExecuteAsync("HSET", "hset_ret", "f1", "updated", "f2", "new");
        Assert.Equal(1, (long)result); // only f2 is new
    }

    // --- IncrByFloat precision ---

    [Fact]
    public async Task IncrByFloat_maintains_precision()
    {
        await _db.StringSetAsync("float_prec", "10.5");
        var result = await _db.StringIncrementAsync("float_prec", 0.1);
        Assert.InRange(result, 10.59, 10.61);
    }

    // --- Empty key after all members removed ---

    [Fact]
    public async Task ZRem_all_members_deletes_key()
    {
        await _db.SortedSetAddAsync("zrem_all", new SortedSetEntry[] { new("a", 1), new("b", 2) });
        await _db.SortedSetRemoveAsync("zrem_all", new RedisValue[] { "a", "b" });
        Assert.Equal(RedisType.None, await _db.KeyTypeAsync("zrem_all"));
    }
}
