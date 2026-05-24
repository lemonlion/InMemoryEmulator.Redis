using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class BlockingCommandTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public BlockingCommandTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task BLPOP_returns_immediately_when_list_has_elements()
    {
        await _db.ListRightPushAsync("blpop_ready", new RedisValue[] { "a", "b" });
        var result = await _db.ExecuteAsync("BLPOP", "blpop_ready", "1");
        Assert.False(result.IsNull);
    }

    [Fact]
    public async Task BRPOP_returns_immediately_when_list_has_elements()
    {
        await _db.ListRightPushAsync("brpop_ready", new RedisValue[] { "a", "b" });
        var result = await _db.ExecuteAsync("BRPOP", "brpop_ready", "1");
        Assert.False(result.IsNull);
    }

    [Fact]
    public async Task BLPOP_blocks_and_returns_when_element_pushed()
    {
        var popTask = Task.Run(async () =>
        {
            var mux = await _fixture.GetMultiplexerAsync();
            var db2 = mux.GetDatabase();
            return await db2.ExecuteAsync("BLPOP", "blpop_wait", "2");
        });

        await Task.Delay(100);
        await _db.ListRightPushAsync("blpop_wait", "arrived");

        var result = await popTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(result);
    }

    [Fact]
    public async Task BLPOP_returns_nil_on_timeout()
    {
        var result = await _db.ExecuteAsync("BLPOP", "blpop_empty", "1");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task BZPOPMIN_returns_immediately_with_elements()
    {
        await _db.SortedSetAddAsync("bzpop_zset", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        var result = await _db.ExecuteAsync("BZPOPMIN", "bzpop_zset", "1");
        Assert.False(result.IsNull);
    }

    [Fact]
    public async Task BZPOPMAX_returns_immediately_with_elements()
    {
        await _db.SortedSetAddAsync("bzpopmax_zset", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        var result = await _db.ExecuteAsync("BZPOPMAX", "bzpopmax_zset", "1");
        Assert.False(result.IsNull);
    }

    [Fact]
    public async Task BZPOPMIN_returns_nil_on_empty_timeout()
    {
        var result = await _db.ExecuteAsync("BZPOPMIN", "bzpop_empty", "1");
        Assert.True(result.IsNull);
    }
}
