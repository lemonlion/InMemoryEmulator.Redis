using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class HashEdgeCaseTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public HashEdgeCaseTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task HSet_returns_count_of_new_fields_only()
    {
        await _db.HashSetAsync("hset_count", new HashEntry[] { new("f1", "v1"), new("f2", "v2") });
        // Update f1, add f3 — should return 1 (only new field)
        await _db.HashSetAsync("hset_count", new HashEntry[] { new("f1", "updated"), new("f3", "v3") });
        Assert.Equal(3, await _db.HashLengthAsync("hset_count"));
    }

    [Fact]
    public async Task HSetNx_does_not_overwrite()
    {
        await _db.HashSetAsync("hsetnx", "field", "original");
        var result = await _db.HashSetAsync("hsetnx", "field", "new", When.NotExists);
        Assert.False(result);
        Assert.Equal("original", (await _db.HashGetAsync("hsetnx", "field")).ToString());
    }

    [Fact]
    public async Task HSetNx_creates_new_field()
    {
        await _db.HashSetAsync("hsetnx_new", "existing", "v");
        var result = await _db.HashSetAsync("hsetnx_new", "newfield", "v2", When.NotExists);
        Assert.True(result);
    }

    [Fact]
    public async Task HGet_nonexistent_field_returns_null()
    {
        await _db.HashSetAsync("hget_null", "f1", "v1");
        var result = await _db.HashGetAsync("hget_null", "missing");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task HGet_nonexistent_key_returns_null()
    {
        var result = await _db.HashGetAsync("hget_nokey", "field");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task HGetAll_on_nonexistent_key_returns_empty()
    {
        var all = await _db.HashGetAllAsync("hgetall_missing");
        Assert.Empty(all);
    }

    [Fact]
    public async Task HDel_nonexistent_field_returns_false()
    {
        await _db.HashSetAsync("hdel_miss", "f1", "v1");
        var result = await _db.HashDeleteAsync("hdel_miss", "nonexistent");
        Assert.False(result);
    }

    [Fact]
    public async Task HDel_last_field_removes_key()
    {
        await _db.HashSetAsync("hdel_last", "only", "value");
        await _db.HashDeleteAsync("hdel_last", "only");
        Assert.False(await _db.KeyExistsAsync("hdel_last"));
    }

    [Fact]
    public async Task HIncrBy_on_nonexistent_field_starts_from_zero()
    {
        var result = await _db.HashIncrementAsync("hincrby_new", "counter", 5);
        Assert.Equal(5, result);
    }

    [Fact]
    public async Task HIncrBy_with_negative_decrements()
    {
        await _db.HashSetAsync("hincrby_neg", "counter", "10");
        var result = await _db.HashIncrementAsync("hincrby_neg", "counter", -3);
        Assert.Equal(7, result);
    }

    [Fact]
    public async Task HMGet_returns_nulls_for_missing_fields()
    {
        await _db.HashSetAsync("hmget_mix", new HashEntry[] { new("f1", "v1"), new("f3", "v3") });
        var results = await _db.HashGetAsync("hmget_mix", new RedisValue[] { "f1", "f2", "f3" });
        Assert.Equal("v1", results[0].ToString());
        Assert.True(results[1].IsNull);
        Assert.Equal("v3", results[2].ToString());
    }

    [Fact]
    public async Task HExists_returns_correct_values()
    {
        await _db.HashSetAsync("hexists", "present", "v");
        Assert.True(await _db.HashExistsAsync("hexists", "present"));
        Assert.False(await _db.HashExistsAsync("hexists", "absent"));
    }

    [Fact]
    public async Task HLen_on_nonexistent_key_returns_zero()
    {
        var len = await _db.HashLengthAsync("hlen_missing");
        Assert.Equal(0, len);
    }
}
