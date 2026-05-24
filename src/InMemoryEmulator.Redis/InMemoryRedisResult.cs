using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;
using StackExchange.Redis;

namespace InMemoryEmulator.Redis;

public sealed class InMemoryRedisResult : IAsyncDisposable
{
    internal InMemoryRedisResult(IConnectionMultiplexer multiplexer, InMemoryRedisStore store,
        FakeRedisServer server, CommandLog commandLog)
    {
        Multiplexer = multiplexer;
        Store = store;
        Server = server;
        CommandLog = commandLog;
    }

    /// <summary>Tier 1: Production-like SDK access.</summary>
    public IConnectionMultiplexer Multiplexer { get; }

    /// <summary>Convenience: IDatabase for db 0.</summary>
    public IDatabase Database => Multiplexer.GetDatabase();

    /// <summary>Tier 2: Direct store access for test setup/assertions.</summary>
    public InMemoryRedisStore Store { get; }

    /// <summary>Tier 3: Server-level access for fault injection and diagnostics.</summary>
    public FakeRedisServer Server { get; }

    /// <summary>Command log for test assertions.</summary>
    public CommandLog CommandLog { get; }

    public async ValueTask DisposeAsync()
    {
        Multiplexer.Dispose();
        await Server.DisposeAsync();
    }
}
