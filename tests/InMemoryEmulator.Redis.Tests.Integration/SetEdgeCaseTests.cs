using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class SetEdgeCaseTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public SetEdgeCaseTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task SAdd_duplicate_members_not_counted()
    {
        var added = await _db.SetAddAsync("sadd_dup", new RedisValue[] { "a", "b", "a", "c", "b" });
        Assert.Equal(3, added);
        Assert.Equal(3, await _db.SetLengthAsync("sadd_dup"));
    }

    [Fact]
    public async Task SRem_nonexistent_member_returns_false()
    {
        await _db.SetAddAsync("srem_miss", "a");
        var removed = await _db.SetRemoveAsync("srem_miss", "nonexistent");
        Assert.False(removed);
    }

    [Fact]
    public async Task SRem_last_member_removes_key()
    {
        await _db.SetAddAsync("srem_last", "only");
        await _db.SetRemoveAsync("srem_last", "only");
        Assert.False(await _db.KeyExistsAsync("srem_last"));
    }

    [Fact]
    public async Task SIsMember_on_nonexistent_key_returns_false()
    {
        Assert.False(await _db.SetContainsAsync("sismem_missing", "member"));
    }

    [Fact]
    public async Task SCard_on_nonexistent_key_returns_zero()
    {
        Assert.Equal(0, await _db.SetLengthAsync("scard_missing"));
    }

    [Fact]
    public async Task SInter_with_nonexistent_key_returns_empty()
    {
        await _db.SetAddAsync("sinter_a", new RedisValue[] { "x", "y", "z" });
        var result = await _db.SetCombineAsync(SetOperation.Intersect, "sinter_a", "sinter_missing");
        Assert.Empty(result);
    }

    [Fact]
    public async Task SInter_disjoint_sets_returns_empty()
    {
        await _db.SetAddAsync("sinter_d1", new RedisValue[] { "a", "b" });
        await _db.SetAddAsync("sinter_d2", new RedisValue[] { "c", "d" });
        var result = await _db.SetCombineAsync(SetOperation.Intersect, "sinter_d1", "sinter_d2");
        Assert.Empty(result);
    }

    [Fact]
    public async Task SUnion_combines_all_unique()
    {
        await _db.SetAddAsync("sunion_1", new RedisValue[] { "a", "b", "c" });
        await _db.SetAddAsync("sunion_2", new RedisValue[] { "b", "c", "d" });
        var result = await _db.SetCombineAsync(SetOperation.Union, "sunion_1", "sunion_2");
        Assert.Equal(4, result.Length);
    }

    [Fact]
    public async Task SUnion_with_nonexistent_returns_existing_set()
    {
        await _db.SetAddAsync("sunion_one", new RedisValue[] { "a", "b" });
        var result = await _db.SetCombineAsync(SetOperation.Union, "sunion_one", "sunion_ghost");
        Assert.Equal(2, result.Length);
    }

    [Fact]
    public async Task SDiff_returns_elements_only_in_first()
    {
        await _db.SetAddAsync("sdiff_1", new RedisValue[] { "a", "b", "c", "d" });
        await _db.SetAddAsync("sdiff_2", new RedisValue[] { "b", "d" });
        var result = await _db.SetCombineAsync(SetOperation.Difference, "sdiff_1", "sdiff_2");
        Assert.Equal(2, result.Length);
        Assert.Contains(result, v => v == "a");
        Assert.Contains(result, v => v == "c");
    }

    [Fact]
    public async Task SDiff_with_nonexistent_subtracts_nothing()
    {
        await _db.SetAddAsync("sdiff_one", new RedisValue[] { "a", "b", "c" });
        var result = await _db.SetCombineAsync(SetOperation.Difference, "sdiff_one", "sdiff_ghost");
        Assert.Equal(3, result.Length);
    }

    [Fact]
    public async Task SMove_moves_member_between_sets()
    {
        await _db.SetAddAsync("smove_src", new RedisValue[] { "a", "b" });
        await _db.SetAddAsync("smove_dst", new RedisValue[] { "c" });
        var moved = await _db.SetMoveAsync("smove_src", "smove_dst", "a");
        Assert.True(moved);
        Assert.False(await _db.SetContainsAsync("smove_src", "a"));
        Assert.True(await _db.SetContainsAsync("smove_dst", "a"));
    }

    [Fact]
    public async Task SMove_nonexistent_member_returns_false()
    {
        await _db.SetAddAsync("smove_fail", "a");
        var moved = await _db.SetMoveAsync("smove_fail", "smove_dst2", "nonexistent");
        Assert.False(moved);
    }
}
