using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class StreamTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public StreamTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task XAdd_and_XLen_work()
    {
        await _db.StreamAddAsync("stream1", new NameValueEntry[] { new("field1", "value1") });
        await _db.StreamAddAsync("stream1", new NameValueEntry[] { new("field2", "value2") });

        var len = await _db.StreamLengthAsync("stream1");
        Assert.Equal(2, len);
    }

    [Fact]
    public async Task XAdd_returns_auto_generated_id()
    {
        var id = await _db.StreamAddAsync("stream2", new NameValueEntry[] { new("k", "v") });
        Assert.False(id.IsNull);
        Assert.Contains("-", id.ToString());
    }

    [Fact]
    public async Task XRange_returns_entries_in_order()
    {
        await _db.StreamAddAsync("stream3", new NameValueEntry[] { new("a", "1") });
        await _db.StreamAddAsync("stream3", new NameValueEntry[] { new("b", "2") });
        await _db.StreamAddAsync("stream3", new NameValueEntry[] { new("c", "3") });

        var entries = await _db.StreamRangeAsync("stream3", "-", "+");
        Assert.Equal(3, entries.Length);
        Assert.Equal("1", entries[0].Values.First(v => v.Name == "a").Value.ToString());
    }

    [Fact]
    public async Task XDel_removes_entry()
    {
        var id = await _db.StreamAddAsync("stream4", new NameValueEntry[] { new("k", "v") });
        await _db.StreamAddAsync("stream4", new NameValueEntry[] { new("k2", "v2") });

        var deleted = await _db.StreamDeleteAsync("stream4", new RedisValue[] { id! });
        Assert.Equal(1, deleted);

        var len = await _db.StreamLengthAsync("stream4");
        Assert.Equal(1, len);
    }
}
