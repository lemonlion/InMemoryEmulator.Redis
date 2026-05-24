using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ExpiryEdgeCaseTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public ExpiryEdgeCaseTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Expire_on_nonexistent_key_returns_false()
    {
        var result = await _db.KeyExpireAsync("expire_missing", TimeSpan.FromSeconds(10));
        Assert.False(result);
    }

    [Fact]
    public async Task Expire_on_existing_key_returns_true()
    {
        await _db.StringSetAsync("expire_exists", "v");
        var result = await _db.KeyExpireAsync("expire_exists", TimeSpan.FromSeconds(100));
        Assert.True(result);
    }

    [Fact]
    public async Task TTL_returns_negative_one_for_no_expiry()
    {
        await _db.StringSetAsync("ttl_noexp", "v");
        var ttl = await _db.KeyTimeToLiveAsync("ttl_noexp");
        Assert.Null(ttl);
    }

    [Fact]
    public async Task TTL_returns_null_for_nonexistent_key()
    {
        var ttl = await _db.KeyTimeToLiveAsync("ttl_missing");
        Assert.Null(ttl);
    }

    [Fact]
    public async Task Key_expires_and_becomes_inaccessible()
    {
        await _db.StringSetAsync("exp_die", "value", TimeSpan.FromMilliseconds(50));
        Assert.True(await _db.KeyExistsAsync("exp_die"));
        await Task.Delay(100);
        Assert.False(await _db.KeyExistsAsync("exp_die"));
    }

    [Fact]
    public async Task Persist_removes_expiry_from_key()
    {
        await _db.StringSetAsync("persist_key", "v", TimeSpan.FromSeconds(100));
        await _db.KeyPersistAsync("persist_key");
        var ttl = await _db.KeyTimeToLiveAsync("persist_key");
        Assert.Null(ttl);
    }

    [Fact]
    public async Task Persist_on_key_without_expiry_returns_false()
    {
        await _db.StringSetAsync("persist_noexp", "v");
        var result = await _db.KeyPersistAsync("persist_noexp");
        Assert.False(result);
    }

    [Fact]
    public async Task Expire_overwrites_existing_expiry()
    {
        await _db.StringSetAsync("exp_overwrite", "v", TimeSpan.FromSeconds(100));
        await _db.KeyExpireAsync("exp_overwrite", TimeSpan.FromSeconds(200));
        var ttl = await _db.KeyTimeToLiveAsync("exp_overwrite");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds > 150);
    }

    [Fact]
    public async Task Set_without_expiry_clears_existing_expiry()
    {
        await _db.StringSetAsync("exp_clear", "v", TimeSpan.FromSeconds(100));
        await _db.StringSetAsync("exp_clear", "v2");
        var ttl = await _db.KeyTimeToLiveAsync("exp_clear");
        Assert.Null(ttl);
    }

    [Fact]
    public async Task Expired_key_type_returns_none()
    {
        await _db.StringSetAsync("exp_type", "v", TimeSpan.FromMilliseconds(50));
        await Task.Delay(100);
        var type = await _db.KeyTypeAsync("exp_type");
        Assert.Equal(RedisType.None, type);
    }

    [Fact]
    public async Task Expired_key_not_returned_by_exists()
    {
        await _db.StringSetAsync("exp_exists", "v", TimeSpan.FromMilliseconds(50));
        await Task.Delay(100);
        Assert.False(await _db.KeyExistsAsync("exp_exists"));
    }

    [Fact]
    public async Task PTTL_has_millisecond_precision()
    {
        await _db.StringSetAsync("pttl_key", "v", TimeSpan.FromMilliseconds(5000));
        var pttl = await _db.ExecuteAsync("PTTL", "pttl_key");
        var ms = (long)pttl;
        Assert.InRange(ms, 4000, 5000);
    }
}
