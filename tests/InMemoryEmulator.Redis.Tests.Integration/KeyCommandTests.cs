using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class KeyCommandTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public KeyCommandTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Del_removes_key()
    {
        await _db.StringSetAsync("delkey", "value");
        var removed = await _db.KeyDeleteAsync("delkey");
        Assert.True(removed);
        Assert.False(await _db.KeyExistsAsync("delkey"));
    }

    [Fact]
    public async Task Exists_returns_correct_value()
    {
        await _db.StringSetAsync("existskey", "value");
        Assert.True(await _db.KeyExistsAsync("existskey"));
        Assert.False(await _db.KeyExistsAsync("nokey"));
    }

    [Fact]
    public async Task Expire_sets_ttl()
    {
        await _db.StringSetAsync("ttlkey", "value");
        await _db.KeyExpireAsync("ttlkey", TimeSpan.FromSeconds(100));
        var ttl = await _db.KeyTimeToLiveAsync("ttlkey");
        Assert.NotNull(ttl);
        Assert.True(ttl.Value.TotalSeconds > 0 && ttl.Value.TotalSeconds <= 100);
    }

    [Fact]
    public async Task Persist_removes_expiry()
    {
        await _db.StringSetAsync("persistkey", "value", TimeSpan.FromSeconds(100));
        await _db.KeyPersistAsync("persistkey");
        var ttl = await _db.KeyTimeToLiveAsync("persistkey");
        Assert.Null(ttl);
    }

    [Fact]
    public async Task Type_returns_correct_type()
    {
        await _db.StringSetAsync("strkey", "value");
        await _db.ListRightPushAsync("listkey", "item");
        await _db.SetAddAsync("setkey", "member");

        Assert.Equal(RedisType.String, await _db.KeyTypeAsync("strkey"));
        Assert.Equal(RedisType.List, await _db.KeyTypeAsync("listkey"));
        Assert.Equal(RedisType.Set, await _db.KeyTypeAsync("setkey"));
    }

    [Fact]
    public async Task Rename_changes_key_name()
    {
        await _db.StringSetAsync("oldname", "value");
        await _db.KeyRenameAsync("oldname", "newname");
        Assert.False(await _db.KeyExistsAsync("oldname"));
        Assert.Equal("value", (await _db.StringGetAsync("newname")).ToString());
    }
}
