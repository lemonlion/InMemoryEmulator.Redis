using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class SortedSetEdgeCaseTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public SortedSetEdgeCaseTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task ZAdd_NX_does_not_update_existing_score()
    {
        await _db.SortedSetAddAsync("znx", "member", 5.0);
        await _db.SortedSetAddAsync("znx", "member", 10.0, When.NotExists);
        var score = await _db.SortedSetScoreAsync("znx", "member");
        Assert.Equal(5.0, score);
    }

    [Fact]
    public async Task ZAdd_XX_only_updates_existing()
    {
        await _db.SortedSetAddAsync("zxx", "existing", 5.0);
        await _db.ExecuteAsync("ZADD", "zxx", "XX", "10", "existing", "20", "newmember");
        Assert.Equal(10.0, await _db.SortedSetScoreAsync("zxx", "existing"));
        Assert.Null(await _db.SortedSetScoreAsync("zxx", "newmember"));
    }

    [Fact]
    public async Task ZScore_nonexistent_member_returns_null()
    {
        await _db.SortedSetAddAsync("zscore_null", "a", 1.0);
        var score = await _db.SortedSetScoreAsync("zscore_null", "missing");
        Assert.Null(score);
    }

    [Fact]
    public async Task ZScore_nonexistent_key_returns_null()
    {
        var score = await _db.SortedSetScoreAsync("zscore_nokey", "member");
        Assert.Null(score);
    }

    [Fact]
    public async Task ZRank_nonexistent_member_returns_null()
    {
        await _db.SortedSetAddAsync("zrank_null", "a", 1.0);
        var rank = await _db.SortedSetRankAsync("zrank_null", "missing");
        Assert.Null(rank);
    }

    [Fact]
    public async Task ZRange_empty_result_for_out_of_bounds()
    {
        await _db.SortedSetAddAsync("zrange_oob", "a", 1.0);
        var results = await _db.SortedSetRangeByRankAsync("zrange_oob", 5, 10);
        Assert.Empty(results);
    }

    [Fact]
    public async Task ZRange_negative_indices()
    {
        await _db.SortedSetAddAsync("zrange_neg", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4)
        });
        var results = await _db.SortedSetRangeByRankAsync("zrange_neg", -2, -1);
        Assert.Equal(2, results.Length);
        Assert.Equal("c", results[0].ToString());
        Assert.Equal("d", results[1].ToString());
    }

    [Fact]
    public async Task ZRangeByScore_with_inf()
    {
        await _db.SortedSetAddAsync("zrangescore_inf", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        var results = await _db.SortedSetRangeByScoreAsync("zrangescore_inf",
            double.NegativeInfinity, double.PositiveInfinity);
        Assert.Equal(3, results.Length);
    }

    [Fact]
    public async Task ZRangeByScore_exclusive_bounds()
    {
        await _db.SortedSetAddAsync("zrangescore_excl", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4)
        });
        var results = await _db.SortedSetRangeByScoreAsync("zrangescore_excl",
            1, 4, Exclude.Both);
        Assert.Equal(2, results.Length);
        Assert.Equal("b", results[0].ToString());
        Assert.Equal("c", results[1].ToString());
    }

    [Fact]
    public async Task ZCount_with_bounds()
    {
        await _db.SortedSetAddAsync("zcount_bounds", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });
        var count = await _db.SortedSetLengthAsync("zcount_bounds", 2, 4);
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task ZCount_returns_zero_for_empty_range()
    {
        await _db.SortedSetAddAsync("zcount_empty", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2)
        });
        var count = await _db.SortedSetLengthAsync("zcount_empty", 10, 20);
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task ZPopMin_removes_lowest_score_element()
    {
        await _db.SortedSetAddAsync("zpop_min", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        var popped = await _db.SortedSetPopAsync("zpop_min", Order.Ascending);
        Assert.NotNull(popped);
        Assert.Equal("a", popped!.Value.Element.ToString());
        Assert.Equal(1.0, popped.Value.Score);
        Assert.Equal(2, await _db.SortedSetLengthAsync("zpop_min"));
    }

    [Fact]
    public async Task ZPopMax_removes_highest_score_element()
    {
        await _db.SortedSetAddAsync("zpop_max", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        var popped = await _db.SortedSetPopAsync("zpop_max", Order.Descending);
        Assert.NotNull(popped);
        Assert.Equal("c", popped!.Value.Element.ToString());
        Assert.Equal(3.0, popped.Value.Score);
    }

    [Fact]
    public async Task ZIncrBy_on_nonexistent_member_creates_it()
    {
        var score = await _db.SortedSetIncrementAsync("zincrby_new", "newmember", 5.0);
        Assert.Equal(5.0, score);
        Assert.Equal(1, await _db.SortedSetLengthAsync("zincrby_new"));
    }

    [Fact]
    public async Task ZRem_nonexistent_member_returns_false()
    {
        await _db.SortedSetAddAsync("zrem_test", "a", 1.0);
        var removed = await _db.SortedSetRemoveAsync("zrem_test", "nonexistent");
        Assert.False(removed);
    }

    [Fact]
    public async Task ZRevRange_returns_descending_order()
    {
        await _db.SortedSetAddAsync("zrevrange", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        var results = await _db.SortedSetRangeByRankAsync("zrevrange", order: Order.Descending);
        Assert.Equal("c", results[0].ToString());
        Assert.Equal("b", results[1].ToString());
        Assert.Equal("a", results[2].ToString());
    }

    [Fact]
    public async Task ZCard_on_nonexistent_key_returns_zero()
    {
        var count = await _db.SortedSetLengthAsync("zcard_missing");
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task ZAdd_duplicate_member_updates_score()
    {
        await _db.SortedSetAddAsync("zdup", "member", 1.0);
        await _db.SortedSetAddAsync("zdup", "member", 5.0);
        var score = await _db.SortedSetScoreAsync("zdup", "member");
        Assert.Equal(5.0, score);
        Assert.Equal(1, await _db.SortedSetLengthAsync("zdup"));
    }
}
