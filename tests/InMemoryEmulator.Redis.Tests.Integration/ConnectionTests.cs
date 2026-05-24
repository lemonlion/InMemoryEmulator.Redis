using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ConnectionTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public ConnectionTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Ping_returns_pong()
    {
        var result = await _db.PingAsync();
        Assert.True(result.TotalMilliseconds >= 0);
    }

    [Fact]
    public async Task Echo_returns_message()
    {
        var mux = await _fixture.GetMultiplexerAsync();
        var server = mux.GetServers().First();
        var result = await server.EchoAsync("hello");
        Assert.Equal("hello", result);
    }

    [Fact]
    public async Task Select_different_database_isolates_keys()
    {
        var mux = await _fixture.GetMultiplexerAsync();
        var db0 = mux.GetDatabase(0);
        var db1 = mux.GetDatabase(1);

        await db0.StringSetAsync("testkey", "db0value");
        var result0 = await db0.StringGetAsync("testkey");
        var result1 = await db1.StringGetAsync("testkey");

        Assert.Equal("db0value", result0.ToString());
        Assert.True(result1.IsNull);
    }

    [Fact]
    public async Task Multiplexer_connects_successfully()
    {
        var mux = await _fixture.GetMultiplexerAsync();
        Assert.True(mux.IsConnected);
    }
}
