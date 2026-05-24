using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ListCommandTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public ListCommandTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task LPush_and_LRange_roundtrips()
    {
        await _db.ListLeftPushAsync("list1", new RedisValue[] { "a", "b", "c" });
        var items = await _db.ListRangeAsync("list1", 0, -1);
        Assert.Equal(3, items.Length);
        Assert.Equal("c", items[0].ToString());
        Assert.Equal("b", items[1].ToString());
        Assert.Equal("a", items[2].ToString());
    }

    [Fact]
    public async Task RPush_and_RPop()
    {
        await _db.ListRightPushAsync("list2", new RedisValue[] { "x", "y", "z" });
        var popped = await _db.ListRightPopAsync("list2");
        Assert.Equal("z", popped.ToString());
    }

    [Fact]
    public async Task LLen_returns_count()
    {
        await _db.ListRightPushAsync("list3", new RedisValue[] { "1", "2", "3" });
        var len = await _db.ListLengthAsync("list3");
        Assert.Equal(3, len);
    }

    [Fact]
    public async Task LIndex_returns_element_at_position()
    {
        await _db.ListRightPushAsync("list4", new RedisValue[] { "a", "b", "c" });
        var item = await _db.ListGetByIndexAsync("list4", 1);
        Assert.Equal("b", item.ToString());
    }

    [Fact]
    public async Task LPop_removes_from_head()
    {
        await _db.ListRightPushAsync("list5", new RedisValue[] { "first", "second" });
        var popped = await _db.ListLeftPopAsync("list5");
        Assert.Equal("first", popped.ToString());
    }
}
