using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class SetCommandTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public SetCommandTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task SAdd_and_SMembers_roundtrips()
    {
        await _db.SetAddAsync("set1", new RedisValue[] { "a", "b", "c" });
        var members = await _db.SetMembersAsync("set1");
        Assert.Equal(3, members.Length);
    }

    [Fact]
    public async Task SIsMember_returns_true_for_existing()
    {
        await _db.SetAddAsync("set2", "member1");
        Assert.True(await _db.SetContainsAsync("set2", "member1"));
        Assert.False(await _db.SetContainsAsync("set2", "nonexistent"));
    }

    [Fact]
    public async Task SRem_removes_member()
    {
        await _db.SetAddAsync("set3", new RedisValue[] { "x", "y" });
        await _db.SetRemoveAsync("set3", "x");
        Assert.False(await _db.SetContainsAsync("set3", "x"));
        Assert.True(await _db.SetContainsAsync("set3", "y"));
    }

    [Fact]
    public async Task SCard_returns_count()
    {
        await _db.SetAddAsync("set4", new RedisValue[] { "1", "2", "3" });
        var count = await _db.SetLengthAsync("set4");
        Assert.Equal(3, count);
    }
}
