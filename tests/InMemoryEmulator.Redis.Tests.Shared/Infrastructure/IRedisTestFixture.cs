using StackExchange.Redis;

namespace InMemoryEmulator.Redis.Tests.Infrastructure;

public interface IRedisTestFixture : IAsyncDisposable
{
    TestTarget Target { get; }
    Task<IDatabase> GetDatabaseAsync(int db = 0);
    Task<IConnectionMultiplexer> GetMultiplexerAsync();
    Task FlushAllAsync();
}
