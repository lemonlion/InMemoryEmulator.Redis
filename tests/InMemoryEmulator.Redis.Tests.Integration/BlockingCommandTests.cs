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

    // Ref: https://redis.io/docs/latest/commands/blmpop/
    //   "BLMPOP is the blocking variant of the LMPOP command."
    [Fact]
    public async Task BLMPOP_pops_from_left_when_list_has_elements()
    {
        await _db.ListRightPushAsync("blmpop_left", new RedisValue[] { "a", "b", "c" });
        var result = await _db.ExecuteAsync("BLMPOP", "1", "1", "blmpop_left", "LEFT");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("blmpop_left", (string)arr[0]!);
        var elements = (RedisResult[])arr[1]!;
        Assert.Single(elements);
        Assert.Equal("a", (string)elements[0]!);
    }

    [Fact]
    public async Task BLMPOP_pops_from_right_when_list_has_elements()
    {
        await _db.ListRightPushAsync("blmpop_right", new RedisValue[] { "a", "b", "c" });
        var result = await _db.ExecuteAsync("BLMPOP", "1", "1", "blmpop_right", "RIGHT");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("blmpop_right", (string)arr[0]!);
        var elements = (RedisResult[])arr[1]!;
        Assert.Single(elements);
        Assert.Equal("c", (string)elements[0]!);
    }

    [Fact]
    public async Task BLMPOP_with_count_pops_multiple_elements()
    {
        await _db.ListRightPushAsync("blmpop_count", new RedisValue[] { "a", "b", "c", "d" });
        var result = await _db.ExecuteAsync("BLMPOP", "1", "1", "blmpop_count", "LEFT", "COUNT", "3");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("blmpop_count", (string)arr[0]!);
        var elements = (RedisResult[])arr[1]!;
        Assert.Equal(3, elements.Length);
        Assert.Equal("a", (string)elements[0]!);
        Assert.Equal("b", (string)elements[1]!);
        Assert.Equal("c", (string)elements[2]!);

        var remaining = await _db.ListRangeAsync("blmpop_count");
        Assert.Single(remaining);
        Assert.Equal("d", (string)remaining[0]!);
    }

    [Fact]
    public async Task BLMPOP_returns_nil_on_timeout()
    {
        var result = await _db.ExecuteAsync("BLMPOP", "1", "1", "blmpop_nonexist", "LEFT");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task BLMPOP_checks_multiple_keys_returns_first_nonempty()
    {
        await _db.ListRightPushAsync("blmpop_multi2", new RedisValue[] { "x", "y" });
        var result = await _db.ExecuteAsync("BLMPOP", "1", "2", "blmpop_multi1", "blmpop_multi2", "LEFT");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("blmpop_multi2", (string)arr[0]!);
    }

    [Fact]
    public async Task BLMPOP_blocks_and_returns_when_element_pushed()
    {
        var mux = await _fixture.GetMultiplexerAsync();
        var endpoint = mux.GetEndPoints()[0];
        var opts = new ConfigurationOptions { AbortOnConnectFail = false };
        opts.EndPoints.Add(endpoint);
        await using var mux2 = await ConnectionMultiplexer.ConnectAsync(opts);
        var db2 = mux2.GetDatabase();

        var popTask = Task.Run(async () =>
            await db2.ExecuteAsync("BLMPOP", "3", "1", "blmpop_block", "LEFT"));

        await Task.Delay(300);
        await _db.ListRightPushAsync("blmpop_block", "arrived");

        var result = await popTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("blmpop_block", (string)arr[0]!);
        var elements = (RedisResult[])arr[1]!;
        Assert.Equal("arrived", (string)elements[0]!);
    }

    [Fact]
    public async Task BLMPOP_removes_key_when_list_becomes_empty()
    {
        await _db.ListRightPushAsync("blmpop_empty_key", "only");
        var result = await _db.ExecuteAsync("BLMPOP", "1", "1", "blmpop_empty_key", "LEFT");
        Assert.False(result.IsNull);
        Assert.False(await _db.KeyExistsAsync("blmpop_empty_key"));
    }
}
