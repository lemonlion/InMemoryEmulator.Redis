using InMemoryEmulator.Redis.LuaScripting;
using StackExchange.Redis;

namespace InMemoryEmulator.Redis.Tests.Infrastructure;

public sealed class InMemoryTestFixture : IRedisTestFixture
{
    private InMemoryRedisResult? _result;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public TestTarget Target => TestTarget.InMemory;

    public async Task<IDatabase> GetDatabaseAsync(int db = 0)
    {
        await EnsureCreatedAsync();
        return _result!.Multiplexer.GetDatabase(db);
    }

    public async Task<IConnectionMultiplexer> GetMultiplexerAsync()
    {
        await EnsureCreatedAsync();
        return _result!.Multiplexer;
    }

    public Task FlushAllAsync()
    {
        _result?.Store.FlushAll();
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_result != null)
            await _result.DisposeAsync();
        _lock.Dispose();
    }

    private async Task EnsureCreatedAsync()
    {
        if (_result != null) return;
        await _lock.WaitAsync();
        try
        {
            // Ref: https://redis.io/docs/latest/commands/eval/
            //   Real Redis always has Lua scripting available; enable it for parity.
            _result ??= (await InMemoryRedis.CreateAsync()).UseLuaScripting();
        }
        finally
        {
            _lock.Release();
        }
    }
}
