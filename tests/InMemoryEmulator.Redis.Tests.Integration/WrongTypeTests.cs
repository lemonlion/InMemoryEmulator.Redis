using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class WrongTypeTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public WrongTypeTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Incr_on_list_throws_wrongtype()
    {
        await _db.ListRightPushAsync("wt_list", "item");
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.StringIncrementAsync("wt_list"));
        Assert.Contains("WRONGTYPE", ex.Message);
    }

    [Fact]
    public async Task LPush_on_string_throws_wrongtype()
    {
        await _db.StringSetAsync("wt_str", "value");
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.ListLeftPushAsync("wt_str", "item"));
        Assert.Contains("WRONGTYPE", ex.Message);
    }

    [Fact]
    public async Task HSet_on_list_throws_wrongtype()
    {
        await _db.ListRightPushAsync("wt_hlist", "item");
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.HashSetAsync("wt_hlist", "field", "value"));
        Assert.Contains("WRONGTYPE", ex.Message);
    }

    [Fact]
    public async Task SAdd_on_hash_throws_wrongtype()
    {
        await _db.HashSetAsync("wt_hash", "field", "value");
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.SetAddAsync("wt_hash", "member"));
        Assert.Contains("WRONGTYPE", ex.Message);
    }

    [Fact]
    public async Task ZAdd_on_string_throws_wrongtype()
    {
        await _db.StringSetAsync("wt_zstr", "value");
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.SortedSetAddAsync("wt_zstr", "member", 1.0));
        Assert.Contains("WRONGTYPE", ex.Message);
    }

    [Fact]
    public async Task Get_on_list_throws_wrongtype()
    {
        await _db.ListRightPushAsync("wt_getlist", "item");
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.StringGetAsync("wt_getlist"));
        Assert.Contains("WRONGTYPE", ex.Message);
    }

    [Fact]
    public async Task LRange_on_string_throws_wrongtype()
    {
        await _db.StringSetAsync("wt_lrstr", "value");
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.ListRangeAsync("wt_lrstr"));
        Assert.Contains("WRONGTYPE", ex.Message);
    }

    [Fact]
    public async Task Append_on_list_throws_wrongtype()
    {
        await _db.ListRightPushAsync("wt_applist", "item");
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.StringAppendAsync("wt_applist", "text"));
        Assert.Contains("WRONGTYPE", ex.Message);
    }

    [Fact]
    public async Task HGet_on_set_throws_wrongtype()
    {
        await _db.SetAddAsync("wt_hgset", "member");
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.HashGetAsync("wt_hgset", "field"));
        Assert.Contains("WRONGTYPE", ex.Message);
    }

    [Fact]
    public async Task Type_still_works_after_wrongtype_error()
    {
        await _db.ListRightPushAsync("wt_after", "item");
        try { await _db.StringGetAsync("wt_after"); } catch { }
        var type = await _db.KeyTypeAsync("wt_after");
        Assert.Equal(RedisType.List, type);
    }
}
