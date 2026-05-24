using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DumpRestoreTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public DumpRestoreTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Dump_and_Restore_roundtrips_string()
    {
        await _db.StringSetAsync("dump_str", "hello world");
        var dump = await _db.ExecuteAsync("DUMP", "dump_str");
        var payload = (byte[])dump!;
        Assert.NotNull(payload);
        Assert.True(payload.Length > 0);

        await _db.KeyDeleteAsync("dump_str");
        await _db.ExecuteAsync("RESTORE", "dump_str", "0", payload);
        var restored = await _db.StringGetAsync("dump_str");
        Assert.Equal("hello world", restored.ToString());
    }

    [Fact]
    public async Task Dump_on_nonexistent_returns_null()
    {
        var result = await _db.ExecuteAsync("DUMP", "dump_missing");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task Restore_with_ttl_sets_expiry()
    {
        await _db.StringSetAsync("dump_ttl", "value");
        var dump = await _db.ExecuteAsync("DUMP", "dump_ttl");
        await _db.KeyDeleteAsync("dump_ttl");

        await _db.ExecuteAsync("RESTORE", "dump_ttl", "5000", (byte[])dump!);
        var ttl = await _db.KeyTimeToLiveAsync("dump_ttl");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalMilliseconds > 3000);
    }

    [Fact]
    public async Task Restore_with_replace_overwrites()
    {
        await _db.StringSetAsync("restore_rep", "original");

        await _db.StringSetAsync("temp_src", "replaced_value");
        var dump = await _db.ExecuteAsync("DUMP", "temp_src");

        await _db.ExecuteAsync("RESTORE", "restore_rep", "0", (byte[])dump!, "REPLACE");
        var val = await _db.StringGetAsync("restore_rep");
        Assert.Equal("replaced_value", val.ToString());
    }
}
