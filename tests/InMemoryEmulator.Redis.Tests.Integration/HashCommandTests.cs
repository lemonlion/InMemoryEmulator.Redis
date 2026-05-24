using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class HashCommandTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public HashCommandTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task HSet_and_HGet_roundtrips()
    {
        await _db.HashSetAsync("hash1", "field1", "value1");
        var result = await _db.HashGetAsync("hash1", "field1");
        Assert.Equal("value1", result.ToString());
    }

    [Fact]
    public async Task HGetAll_returns_all_fields()
    {
        await _db.HashSetAsync("hash2", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        var all = await _db.HashGetAllAsync("hash2");
        Assert.Equal(2, all.Length);
    }

    [Fact]
    public async Task HDel_removes_field()
    {
        await _db.HashSetAsync("hash3", "field", "value");
        await _db.HashDeleteAsync("hash3", "field");
        var result = await _db.HashGetAsync("hash3", "field");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task HIncrBy_increments_field()
    {
        await _db.HashSetAsync("hash4", "counter", "10");
        var result = await _db.HashIncrementAsync("hash4", "counter", 5);
        Assert.Equal(15, result);
    }

    [Fact]
    public async Task HLen_returns_field_count()
    {
        await _db.HashSetAsync("hash5", new HashEntry[] { new("f1", "v1"), new("f2", "v2"), new("f3", "v3") });
        var len = await _db.HashLengthAsync("hash5");
        Assert.Equal(3, len);
    }
}
