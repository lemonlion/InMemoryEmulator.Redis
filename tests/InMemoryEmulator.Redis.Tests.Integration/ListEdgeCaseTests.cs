using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ListEdgeCaseTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public ListEdgeCaseTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task LRange_on_nonexistent_returns_empty()
    {
        var items = await _db.ListRangeAsync("lrange_missing");
        Assert.Empty(items);
    }

    [Fact]
    public async Task LRange_with_negative_indices()
    {
        await _db.ListRightPushAsync("lrange_neg", new RedisValue[] { "a", "b", "c", "d", "e" });
        var items = await _db.ListRangeAsync("lrange_neg", -3, -1);
        Assert.Equal(3, items.Length);
        Assert.Equal("c", items[0].ToString());
        Assert.Equal("d", items[1].ToString());
        Assert.Equal("e", items[2].ToString());
    }

    [Fact]
    public async Task LIndex_out_of_bounds_returns_null()
    {
        await _db.ListRightPushAsync("lindex_oob", "item");
        var result = await _db.ListGetByIndexAsync("lindex_oob", 5);
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task LIndex_negative_from_end()
    {
        await _db.ListRightPushAsync("lindex_neg", new RedisValue[] { "a", "b", "c" });
        var result = await _db.ListGetByIndexAsync("lindex_neg", -1);
        Assert.Equal("c", result.ToString());
    }

    [Fact]
    public async Task LPop_on_empty_list_returns_null()
    {
        var result = await _db.ListLeftPopAsync("lpop_missing");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task RPop_on_empty_list_returns_null()
    {
        var result = await _db.ListRightPopAsync("rpop_missing");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task Pop_last_element_removes_key()
    {
        await _db.ListRightPushAsync("pop_last", "only");
        await _db.ListLeftPopAsync("pop_last");
        Assert.False(await _db.KeyExistsAsync("pop_last"));
    }

    [Fact]
    public async Task LLen_on_nonexistent_returns_zero()
    {
        var len = await _db.ListLengthAsync("llen_missing");
        Assert.Equal(0, len);
    }

    [Fact]
    public async Task LPushX_on_nonexistent_does_nothing()
    {
        var len = await _db.ListLeftPushAsync("lpushx_missing", "item", When.Exists);
        Assert.Equal(0, len);
        Assert.False(await _db.KeyExistsAsync("lpushx_missing"));
    }

    [Fact]
    public async Task RPushX_on_nonexistent_does_nothing()
    {
        var len = await _db.ListRightPushAsync("rpushx_missing", "item", When.Exists);
        Assert.Equal(0, len);
        Assert.False(await _db.KeyExistsAsync("rpushx_missing"));
    }

    [Fact]
    public async Task LTrim_removes_elements_outside_range()
    {
        await _db.ListRightPushAsync("ltrim_test", new RedisValue[] { "a", "b", "c", "d", "e" });
        await _db.ListTrimAsync("ltrim_test", 1, 3);
        var items = await _db.ListRangeAsync("ltrim_test");
        Assert.Equal(3, items.Length);
        Assert.Equal("b", items[0].ToString());
        Assert.Equal("c", items[1].ToString());
        Assert.Equal("d", items[2].ToString());
    }

    [Fact]
    public async Task LInsert_before_pivot()
    {
        await _db.ListRightPushAsync("linsert_before", new RedisValue[] { "a", "c" });
        await _db.ListInsertBeforeAsync("linsert_before", "c", "b");
        var items = await _db.ListRangeAsync("linsert_before");
        Assert.Equal(3, items.Length);
        Assert.Equal("a", items[0].ToString());
        Assert.Equal("b", items[1].ToString());
        Assert.Equal("c", items[2].ToString());
    }

    [Fact]
    public async Task LInsert_after_pivot()
    {
        await _db.ListRightPushAsync("linsert_after", new RedisValue[] { "a", "c" });
        await _db.ListInsertAfterAsync("linsert_after", "a", "b");
        var items = await _db.ListRangeAsync("linsert_after");
        Assert.Equal(3, items.Length);
        Assert.Equal("a", items[0].ToString());
        Assert.Equal("b", items[1].ToString());
        Assert.Equal("c", items[2].ToString());
    }

    [Fact]
    public async Task LRem_removes_first_occurrence_positive_count()
    {
        await _db.ListRightPushAsync("lrem_pos", new RedisValue[] { "a", "b", "a", "c", "a" });
        var removed = await _db.ListRemoveAsync("lrem_pos", "a", 2);
        Assert.Equal(2, removed);
        var items = await _db.ListRangeAsync("lrem_pos");
        Assert.Equal(3, items.Length);
        Assert.Equal("b", items[0].ToString());
    }

    [Fact]
    public async Task LRem_removes_from_tail_negative_count()
    {
        await _db.ListRightPushAsync("lrem_neg", new RedisValue[] { "a", "b", "a", "c", "a" });
        var removed = await _db.ListRemoveAsync("lrem_neg", "a", -2);
        Assert.Equal(2, removed);
        var items = await _db.ListRangeAsync("lrem_neg");
        Assert.Equal(3, items.Length);
        Assert.Equal("a", items[0].ToString());
    }

    [Fact]
    public async Task LRem_removes_all_with_zero_count()
    {
        await _db.ListRightPushAsync("lrem_zero", new RedisValue[] { "a", "b", "a", "c", "a" });
        var removed = await _db.ListRemoveAsync("lrem_zero", "a", 0);
        Assert.Equal(3, removed);
        var items = await _db.ListRangeAsync("lrem_zero");
        Assert.Equal(2, items.Length);
    }
}
