using StackExchange.Redis;

namespace InMemoryEmulator.Redis.Tests.Infrastructure;

public sealed class DockerTestFixture : IRedisTestFixture
{
    private readonly RedisSession _session;
    private IConnectionMultiplexer? _multiplexer;

    public DockerTestFixture(RedisSession session) => _session = session;

    public TestTarget Target => TestTarget.Docker;

    public async Task<IDatabase> GetDatabaseAsync(int db = 0)
    {
        var mux = await GetMultiplexerAsync();
        return mux.GetDatabase(db);
    }

    public async Task<IConnectionMultiplexer> GetMultiplexerAsync()
    {
        if (_multiplexer == null)
        {
            var options = ConfigurationOptions.Parse(_session.DockerConnectionString!);
            options.AllowAdmin = true;
            _multiplexer = await ConnectionMultiplexer.ConnectAsync(options);
        }
        return _multiplexer;
    }

    public async Task FlushAllAsync()
    {
        if (_multiplexer != null)
        {
            var server = _multiplexer.GetServers().First();
            await server.FlushAllDatabasesAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_multiplexer != null)
        {
            await FlushAllAsync();
            _multiplexer.Dispose();
        }
    }
}
