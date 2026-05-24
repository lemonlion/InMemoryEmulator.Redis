using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class SortedSetCommandTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public SortedSetCommandTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task ZAdd_and_ZScore()
    {
        await _db.SortedSetAddAsync("zset1", "member1", 1.5);
        var score = await _db.SortedSetScoreAsync("zset1", "member1");
        Assert.Equal(1.5, score);
    }

    [Fact]
    public async Task ZRange_returns_sorted_members()
    {
        await _db.SortedSetAddAsync("zset2", new SortedSetEntry[]
        {
            new("c", 3), new("a", 1), new("b", 2)
        });
        var members = await _db.SortedSetRangeByRankAsync("zset2");
        Assert.Equal("a", members[0].ToString());
        Assert.Equal("b", members[1].ToString());
        Assert.Equal("c", members[2].ToString());
    }

    [Fact]
    public async Task ZRank_returns_position()
    {
        await _db.SortedSetAddAsync("zset3", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        var rank = await _db.SortedSetRankAsync("zset3", "b");
        Assert.Equal(1, rank);
    }

    [Fact]
    public async Task ZIncrBy_increments_score()
    {
        await _db.SortedSetAddAsync("zset4", "member", 10);
        var newScore = await _db.SortedSetIncrementAsync("zset4", "member", 5);
        Assert.Equal(15, newScore);
    }

    [Fact]
    public async Task ZCard_returns_count()
    {
        await _db.SortedSetAddAsync("zset5", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2)
        });
        var count = await _db.SortedSetLengthAsync("zset5");
        Assert.Equal(2, count);
    }
}
