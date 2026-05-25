using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class SortedSetEnhancementTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public SortedSetEnhancementTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // =====================================================================
    // ZREMRANGEBYLEX
    // Ref: https://redis.io/docs/latest/commands/zremrangebylex/
    // =====================================================================

    [Fact]
    public async Task ZRemRangeByLex_removes_members_in_lex_range()
    {
        await _db.SortedSetAddAsync("zrrl1", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        var removed = await _db.ExecuteAsync("ZREMRANGEBYLEX", "zrrl1", "[b", "[d");
        Assert.Equal(3, (long)removed);

        var remaining = await _db.SortedSetRangeByRankAsync("zrrl1");
        Assert.Equal(2, remaining.Length);
        Assert.Equal("a", remaining[0].ToString());
        Assert.Equal("e", remaining[1].ToString());
    }

    [Fact]
    public async Task ZRemRangeByLex_exclusive_bounds()
    {
        await _db.SortedSetAddAsync("zrrl2", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        var removed = await _db.ExecuteAsync("ZREMRANGEBYLEX", "zrrl2", "(a", "(e");
        Assert.Equal(3, (long)removed);

        var remaining = await _db.SortedSetRangeByRankAsync("zrrl2");
        Assert.Equal(2, remaining.Length);
        Assert.Equal("a", remaining[0].ToString());
        Assert.Equal("e", remaining[1].ToString());
    }

    [Fact]
    public async Task ZRemRangeByLex_full_range_removes_all()
    {
        await _db.SortedSetAddAsync("zrrl3", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0)
        });

        var removed = await _db.ExecuteAsync("ZREMRANGEBYLEX", "zrrl3", "-", "+");
        Assert.Equal(3, (long)removed);
        Assert.False(await _db.KeyExistsAsync("zrrl3"));
    }

    [Fact]
    public async Task ZRemRangeByLex_nonexistent_key_returns_zero()
    {
        var removed = await _db.ExecuteAsync("ZREMRANGEBYLEX", "zrrl_nokey", "[a", "[z");
        Assert.Equal(0, (long)removed);
    }

    // =====================================================================
    // ZREMRANGEBYRANK
    // Ref: https://redis.io/docs/latest/commands/zremrangebyrank/
    // =====================================================================

    [Fact]
    public async Task ZRemRangeByRank_removes_by_index()
    {
        await _db.SortedSetAddAsync("zrrr1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var removed = await _db.SortedSetRemoveRangeByRankAsync("zrrr1", 1, 3);
        Assert.Equal(3, removed);

        var remaining = await _db.SortedSetRangeByRankAsync("zrrr1");
        Assert.Equal(2, remaining.Length);
        Assert.Equal("a", remaining[0].ToString());
        Assert.Equal("e", remaining[1].ToString());
    }

    [Fact]
    public async Task ZRemRangeByRank_negative_indices()
    {
        await _db.SortedSetAddAsync("zrrr2", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        // Remove last two elements
        var removed = await _db.SortedSetRemoveRangeByRankAsync("zrrr2", -2, -1);
        Assert.Equal(2, removed);

        var remaining = await _db.SortedSetRangeByRankAsync("zrrr2");
        Assert.Single(remaining);
        Assert.Equal("a", remaining[0].ToString());
    }

    [Fact]
    public async Task ZRemRangeByRank_removes_all_deletes_key()
    {
        await _db.SortedSetAddAsync("zrrr3", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2)
        });

        var removed = await _db.SortedSetRemoveRangeByRankAsync("zrrr3", 0, -1);
        Assert.Equal(2, removed);
        Assert.False(await _db.KeyExistsAsync("zrrr3"));
    }

    // =====================================================================
    // ZREMRANGEBYSCORE
    // Ref: https://redis.io/docs/latest/commands/zremrangebyscore/
    // =====================================================================

    [Fact]
    public async Task ZRemRangeByScore_removes_by_score()
    {
        await _db.SortedSetAddAsync("zrrs1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var removed = await _db.SortedSetRemoveRangeByScoreAsync("zrrs1", 2, 4);
        Assert.Equal(3, removed);

        var remaining = await _db.SortedSetRangeByRankAsync("zrrs1");
        Assert.Equal(2, remaining.Length);
        Assert.Equal("a", remaining[0].ToString());
        Assert.Equal("e", remaining[1].ToString());
    }

    [Fact]
    public async Task ZRemRangeByScore_exclusive_bounds()
    {
        await _db.SortedSetAddAsync("zrrs2", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var removed = await _db.SortedSetRemoveRangeByScoreAsync("zrrs2", 2, 4, Exclude.Both);
        Assert.Equal(1, removed);

        var remaining = await _db.SortedSetRangeByRankAsync("zrrs2");
        Assert.Equal(4, remaining.Length);
    }

    [Fact]
    public async Task ZRemRangeByScore_inf_removes_all()
    {
        await _db.SortedSetAddAsync("zrrs3", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var removed = await _db.SortedSetRemoveRangeByScoreAsync("zrrs3",
            double.NegativeInfinity, double.PositiveInfinity);
        Assert.Equal(3, removed);
        Assert.False(await _db.KeyExistsAsync("zrrs3"));
    }

    [Fact]
    public async Task ZRemRangeByScore_nonexistent_key_returns_zero()
    {
        var removed = await _db.SortedSetRemoveRangeByScoreAsync("zrrs_nokey", 0, 100);
        Assert.Equal(0, removed);
    }

    // =====================================================================
    // ZDIFF
    // Ref: https://redis.io/docs/latest/commands/zdiff/
    // =====================================================================

    [Fact]
    public async Task ZDiff_returns_difference()
    {
        await _db.SortedSetAddAsync("zdiff1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        await _db.SortedSetAddAsync("zdiff2", new SortedSetEntry[]
        {
            new("b", 5), new("d", 4)
        });

        var result = await _db.SortedSetCombineAsync(SetOperation.Difference, new RedisKey[] { "zdiff1", "zdiff2" });
        Assert.Equal(2, result.Length);
        Assert.Equal("a", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
    }

    [Fact]
    public async Task ZDiff_with_scores()
    {
        await _db.SortedSetAddAsync("zdiffs1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        await _db.SortedSetAddAsync("zdiffs2", new SortedSetEntry[]
        {
            new("b", 5)
        });

        var result = await _db.SortedSetCombineWithScoresAsync(SetOperation.Difference, new RedisKey[] { "zdiffs1", "zdiffs2" });
        Assert.Equal(2, result.Length);
        Assert.Equal("a", result[0].Element.ToString());
        Assert.Equal(1.0, result[0].Score);
        Assert.Equal("c", result[1].Element.ToString());
        Assert.Equal(3.0, result[1].Score);
    }

    [Fact]
    public async Task ZDiff_nonexistent_second_key_returns_all()
    {
        await _db.SortedSetAddAsync("zdiff3", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2)
        });

        var result = await _db.SortedSetCombineAsync(SetOperation.Difference, new RedisKey[] { "zdiff3", "zdiff_nokey" });
        Assert.Equal(2, result.Length);
    }

    // =====================================================================
    // ZUNION
    // Ref: https://redis.io/docs/latest/commands/zunion/
    // =====================================================================

    [Fact]
    public async Task ZUnion_returns_union()
    {
        await _db.SortedSetAddAsync("zunion1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2)
        });
        await _db.SortedSetAddAsync("zunion2", new SortedSetEntry[]
        {
            new("b", 3), new("c", 4)
        });

        var result = await _db.SortedSetCombineAsync(SetOperation.Union, new RedisKey[] { "zunion1", "zunion2" });
        Assert.Equal(3, result.Length);
        // Sorted by score: a(1), b(2+3=5), c(4) => a, c, b
        Assert.Equal("a", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
        Assert.Equal("b", result[2].ToString());
    }

    [Fact]
    public async Task ZUnion_with_scores()
    {
        await _db.SortedSetAddAsync("zunions1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2)
        });
        await _db.SortedSetAddAsync("zunions2", new SortedSetEntry[]
        {
            new("b", 3), new("c", 4)
        });

        var result = await _db.SortedSetCombineWithScoresAsync(SetOperation.Union, new RedisKey[] { "zunions1", "zunions2" });
        Assert.Equal(3, result.Length);
        // a=1, c=4, b=5 (sorted by score)
        Assert.Equal("a", result[0].Element.ToString());
        Assert.Equal(1.0, result[0].Score);
        Assert.Equal("c", result[1].Element.ToString());
        Assert.Equal(4.0, result[1].Score);
        Assert.Equal("b", result[2].Element.ToString());
        Assert.Equal(5.0, result[2].Score);
    }

    [Fact]
    public async Task ZUnion_with_nonexistent_key()
    {
        await _db.SortedSetAddAsync("zunion3", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2)
        });

        var result = await _db.SortedSetCombineAsync(SetOperation.Union, new RedisKey[] { "zunion3", "zunion_nokey" });
        Assert.Equal(2, result.Length);
    }

    // =====================================================================
    // ZINTER
    // Ref: https://redis.io/docs/latest/commands/zinter/
    // =====================================================================

    [Fact]
    public async Task ZInter_returns_intersection()
    {
        await _db.SortedSetAddAsync("zinter1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        await _db.SortedSetAddAsync("zinter2", new SortedSetEntry[]
        {
            new("b", 5), new("c", 6), new("d", 7)
        });

        var result = await _db.SortedSetCombineAsync(SetOperation.Intersect, new RedisKey[] { "zinter1", "zinter2" });
        Assert.Equal(2, result.Length);
        // b(2+5=7), c(3+6=9) => b, c
        Assert.Equal("b", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
    }

    [Fact]
    public async Task ZInter_with_scores()
    {
        await _db.SortedSetAddAsync("zinters1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        await _db.SortedSetAddAsync("zinters2", new SortedSetEntry[]
        {
            new("b", 10), new("c", 20)
        });

        var result = await _db.SortedSetCombineWithScoresAsync(SetOperation.Intersect, new RedisKey[] { "zinters1", "zinters2" });
        Assert.Equal(2, result.Length);
        Assert.Equal("b", result[0].Element.ToString());
        Assert.Equal(12.0, result[0].Score);
        Assert.Equal("c", result[1].Element.ToString());
        Assert.Equal(23.0, result[1].Score);
    }

    [Fact]
    public async Task ZInter_empty_when_no_overlap()
    {
        await _db.SortedSetAddAsync("zinter3", new SortedSetEntry[] { new("a", 1) });
        await _db.SortedSetAddAsync("zinter4", new SortedSetEntry[] { new("b", 2) });

        var result = await _db.SortedSetCombineAsync(SetOperation.Intersect, new RedisKey[] { "zinter3", "zinter4" });
        Assert.Empty(result);
    }

    // =====================================================================
    // ZINTERCARD
    // Ref: https://redis.io/docs/latest/commands/zintercard/
    // =====================================================================

    [Fact]
    public async Task ZInterCard_returns_intersection_cardinality()
    {
        await _db.SortedSetAddAsync("zic1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        await _db.SortedSetAddAsync("zic2", new SortedSetEntry[]
        {
            new("b", 5), new("c", 6), new("d", 7)
        });

        var count = await _db.SortedSetIntersectionLengthAsync(new RedisKey[] { "zic1", "zic2" });
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task ZInterCard_with_limit()
    {
        await _db.SortedSetAddAsync("zic3", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4)
        });
        await _db.SortedSetAddAsync("zic4", new SortedSetEntry[]
        {
            new("a", 5), new("b", 6), new("c", 7), new("d", 8)
        });

        var count = await _db.SortedSetIntersectionLengthAsync(new RedisKey[] { "zic3", "zic4" }, limit: 2);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task ZInterCard_nonexistent_key_returns_zero()
    {
        await _db.SortedSetAddAsync("zic5", new SortedSetEntry[] { new("a", 1) });

        var count = await _db.SortedSetIntersectionLengthAsync(new RedisKey[] { "zic5", "zic_nokey" });
        Assert.Equal(0, count);
    }

    // =====================================================================
    // ZUNIONSTORE / ZINTERSTORE with WEIGHTS and AGGREGATE
    // Ref: https://redis.io/docs/latest/commands/zunionstore/
    // Ref: https://redis.io/docs/latest/commands/zinterstore/
    // =====================================================================

    [Fact]
    public async Task ZUnionStore_with_weights()
    {
        await _db.SortedSetAddAsync("zuw1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2)
        });
        await _db.SortedSetAddAsync("zuw2", new SortedSetEntry[]
        {
            new("b", 3), new("c", 4)
        });

        var result = await _db.ExecuteAsync("ZUNIONSTORE", "zuw_dest", "2", "zuw1", "zuw2", "WEIGHTS", "2", "3");
        Assert.Equal(3, (long)result);

        // a = 1*2 = 2, b = 2*2 + 3*3 = 13, c = 4*3 = 12
        Assert.Equal(2.0, await _db.SortedSetScoreAsync("zuw_dest", "a"));
        Assert.Equal(13.0, await _db.SortedSetScoreAsync("zuw_dest", "b"));
        Assert.Equal(12.0, await _db.SortedSetScoreAsync("zuw_dest", "c"));
    }

    [Fact]
    public async Task ZUnionStore_with_aggregate_min()
    {
        await _db.SortedSetAddAsync("zuam1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 10)
        });
        await _db.SortedSetAddAsync("zuam2", new SortedSetEntry[]
        {
            new("b", 3), new("c", 4)
        });

        var result = await _db.ExecuteAsync("ZUNIONSTORE", "zuam_dest", "2", "zuam1", "zuam2", "AGGREGATE", "MIN");
        Assert.Equal(3, (long)result);

        Assert.Equal(1.0, await _db.SortedSetScoreAsync("zuam_dest", "a"));
        Assert.Equal(3.0, await _db.SortedSetScoreAsync("zuam_dest", "b"));
        Assert.Equal(4.0, await _db.SortedSetScoreAsync("zuam_dest", "c"));
    }

    [Fact]
    public async Task ZUnionStore_with_aggregate_max()
    {
        await _db.SortedSetAddAsync("zuax1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 10)
        });
        await _db.SortedSetAddAsync("zuax2", new SortedSetEntry[]
        {
            new("b", 3), new("c", 4)
        });

        var result = await _db.ExecuteAsync("ZUNIONSTORE", "zuax_dest", "2", "zuax1", "zuax2", "AGGREGATE", "MAX");
        Assert.Equal(3, (long)result);

        Assert.Equal(1.0, await _db.SortedSetScoreAsync("zuax_dest", "a"));
        Assert.Equal(10.0, await _db.SortedSetScoreAsync("zuax_dest", "b"));
        Assert.Equal(4.0, await _db.SortedSetScoreAsync("zuax_dest", "c"));
    }

    [Fact]
    public async Task ZInterStore_with_weights()
    {
        await _db.SortedSetAddAsync("ziw1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        await _db.SortedSetAddAsync("ziw2", new SortedSetEntry[]
        {
            new("b", 4), new("c", 5)
        });

        var result = await _db.ExecuteAsync("ZINTERSTORE", "ziw_dest", "2", "ziw1", "ziw2", "WEIGHTS", "2", "3");
        Assert.Equal(2, (long)result);

        // b = 2*2 + 4*3 = 16, c = 3*2 + 5*3 = 21
        Assert.Equal(16.0, await _db.SortedSetScoreAsync("ziw_dest", "b"));
        Assert.Equal(21.0, await _db.SortedSetScoreAsync("ziw_dest", "c"));
    }

    [Fact]
    public async Task ZInterStore_with_aggregate_min()
    {
        await _db.SortedSetAddAsync("ziam1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 10)
        });
        await _db.SortedSetAddAsync("ziam2", new SortedSetEntry[]
        {
            new("a", 5), new("b", 3)
        });

        var result = await _db.ExecuteAsync("ZINTERSTORE", "ziam_dest", "2", "ziam1", "ziam2", "AGGREGATE", "MIN");
        Assert.Equal(2, (long)result);

        Assert.Equal(1.0, await _db.SortedSetScoreAsync("ziam_dest", "a"));
        Assert.Equal(3.0, await _db.SortedSetScoreAsync("ziam_dest", "b"));
    }

    [Fact]
    public async Task ZInterStore_with_weights_and_aggregate_max()
    {
        await _db.SortedSetAddAsync("ziwa1", new SortedSetEntry[]
        {
            new("a", 2), new("b", 4)
        });
        await _db.SortedSetAddAsync("ziwa2", new SortedSetEntry[]
        {
            new("a", 3), new("b", 1)
        });

        var result = await _db.ExecuteAsync("ZINTERSTORE", "ziwa_dest", "2", "ziwa1", "ziwa2", "WEIGHTS", "2", "3", "AGGREGATE", "MAX");
        Assert.Equal(2, (long)result);

        // a: max(2*2, 3*3) = max(4, 9) = 9
        // b: max(4*2, 1*3) = max(8, 3) = 8
        Assert.Equal(9.0, await _db.SortedSetScoreAsync("ziwa_dest", "a"));
        Assert.Equal(8.0, await _db.SortedSetScoreAsync("ziwa_dest", "b"));
    }

    // =====================================================================
    // ZRANGE extended syntax (BYSCORE, BYLEX, REV, LIMIT)
    // Ref: https://redis.io/docs/latest/commands/zrange/
    // =====================================================================

    [Fact]
    public async Task ZRange_byscore_returns_score_range()
    {
        await _db.SortedSetAddAsync("zrbs1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.ExecuteAsync("ZRANGE", "zrbs1", "2", "4", "BYSCORE");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal("b", (string)arr[0]!);
        Assert.Equal("c", (string)arr[1]!);
        Assert.Equal("d", (string)arr[2]!);
    }

    [Fact]
    public async Task ZRange_byscore_with_inf()
    {
        await _db.SortedSetAddAsync("zrbs2", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var result = await _db.ExecuteAsync("ZRANGE", "zrbs2", "-inf", "+inf", "BYSCORE");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
    }

    [Fact]
    public async Task ZRange_byscore_withscores()
    {
        await _db.SortedSetAddAsync("zrbs3", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var result = await _db.ExecuteAsync("ZRANGE", "zrbs3", "1", "2", "BYSCORE", "WITHSCORES");
        var arr = (RedisResult[])result!;
        Assert.Equal(4, arr.Length); // 2 members * 2 (member + score)
        Assert.Equal("a", (string)arr[0]!);
        Assert.Equal("1", (string)arr[1]!);
        Assert.Equal("b", (string)arr[2]!);
        Assert.Equal("2", (string)arr[3]!);
    }

    [Fact]
    public async Task ZRange_byscore_rev()
    {
        await _db.SortedSetAddAsync("zrbs4", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        // REV with BYSCORE: max is first arg, min is second arg
        var result = await _db.ExecuteAsync("ZRANGE", "zrbs4", "4", "2", "BYSCORE", "REV");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal("d", (string)arr[0]!);
        Assert.Equal("c", (string)arr[1]!);
        Assert.Equal("b", (string)arr[2]!);
    }

    [Fact]
    public async Task ZRange_byscore_limit()
    {
        await _db.SortedSetAddAsync("zrbs5", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.ExecuteAsync("ZRANGE", "zrbs5", "1", "5", "BYSCORE", "LIMIT", "1", "2");
        var arr = (RedisResult[])result!;
        Assert.Equal(2, arr.Length);
        Assert.Equal("b", (string)arr[0]!);
        Assert.Equal("c", (string)arr[1]!);
    }

    [Fact]
    public async Task ZRange_bylex()
    {
        await _db.SortedSetAddAsync("zrbl1", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        var result = await _db.ExecuteAsync("ZRANGE", "zrbl1", "[b", "[d", "BYLEX");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal("b", (string)arr[0]!);
        Assert.Equal("c", (string)arr[1]!);
        Assert.Equal("d", (string)arr[2]!);
    }

    [Fact]
    public async Task ZRange_bylex_rev()
    {
        await _db.SortedSetAddAsync("zrbl2", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        // REV with BYLEX: max is first arg, min is second arg
        var result = await _db.ExecuteAsync("ZRANGE", "zrbl2", "[d", "[b", "BYLEX", "REV");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal("d", (string)arr[0]!);
        Assert.Equal("c", (string)arr[1]!);
        Assert.Equal("b", (string)arr[2]!);
    }

    [Fact]
    public async Task ZRange_rev_by_rank()
    {
        await _db.SortedSetAddAsync("zrrev1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var result = await _db.ExecuteAsync("ZRANGE", "zrrev1", "0", "-1", "REV");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal("c", (string)arr[0]!);
        Assert.Equal("b", (string)arr[1]!);
        Assert.Equal("a", (string)arr[2]!);
    }

    // =====================================================================
    // ZRANK/ZREVRANK WITHSCORE (Redis 7.2)
    // Ref: https://redis.io/docs/latest/commands/zrank/
    // =====================================================================

    [Fact]
    public async Task ZRank_withscore_returns_rank_and_score()
    {
        await _db.SortedSetAddAsync("zrws1", new SortedSetEntry[]
        {
            new("a", 10), new("b", 20), new("c", 30)
        });

        var result = await _db.ExecuteAsync("ZRANK", "zrws1", "b", "WITHSCORE");
        var arr = (RedisResult[])result!;
        Assert.Equal(2, arr.Length);
        Assert.Equal(1, (long)arr[0]); // rank
        Assert.Equal("20", (string)arr[1]!); // score
    }

    [Fact]
    public async Task ZRank_withscore_nonexistent_member_returns_nil()
    {
        await _db.SortedSetAddAsync("zrws2", new SortedSetEntry[]
        {
            new("a", 10)
        });

        var result = await _db.ExecuteAsync("ZRANK", "zrws2", "missing", "WITHSCORE");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task ZRevRank_withscore_returns_rank_and_score()
    {
        await _db.SortedSetAddAsync("zrrws1", new SortedSetEntry[]
        {
            new("a", 10), new("b", 20), new("c", 30)
        });

        var result = await _db.ExecuteAsync("ZREVRANK", "zrrws1", "b", "WITHSCORE");
        var arr = (RedisResult[])result!;
        Assert.Equal(2, arr.Length);
        Assert.Equal(1, (long)arr[0]); // reverse rank
        Assert.Equal("20", (string)arr[1]!); // score
    }

    // =====================================================================
    // ZMPOP
    // Ref: https://redis.io/docs/latest/commands/zmpop/
    // =====================================================================

    [Fact]
    public async Task ZMPop_min_pops_lowest()
    {
        await _db.SortedSetAddAsync("zmpop1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var result = await _db.ExecuteAsync("ZMPOP", "1", "zmpop1", "MIN");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("zmpop1", (string)arr[0]!);
        var elements = (RedisResult[])arr[1]!;
        Assert.Single(elements);
        var entry = (RedisResult[])elements[0]!;
        Assert.Equal("a", (string)entry[0]!);
        Assert.Equal("1", (string)entry[1]!);

        Assert.Equal(2, await _db.SortedSetLengthAsync("zmpop1"));
    }

    [Fact]
    public async Task ZMPop_max_pops_highest()
    {
        await _db.SortedSetAddAsync("zmpop2", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var result = await _db.ExecuteAsync("ZMPOP", "1", "zmpop2", "MAX");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        var elements = (RedisResult[])arr[1]!;
        var entry = (RedisResult[])elements[0]!;
        Assert.Equal("c", (string)entry[0]!);
        Assert.Equal("3", (string)entry[1]!);
    }

    [Fact]
    public async Task ZMPop_with_count()
    {
        await _db.SortedSetAddAsync("zmpop3", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4)
        });

        var result = await _db.ExecuteAsync("ZMPOP", "1", "zmpop3", "MIN", "COUNT", "3");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        var elements = (RedisResult[])arr[1]!;
        Assert.Equal(3, elements.Length);

        Assert.Equal(1, await _db.SortedSetLengthAsync("zmpop3"));
    }

    [Fact]
    public async Task ZMPop_nonexistent_returns_nil()
    {
        var result = await _db.ExecuteAsync("ZMPOP", "1", "zmpop_nokey", "MIN");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task ZMPop_multiple_keys_pops_from_first_nonempty()
    {
        await _db.SortedSetAddAsync("zmpop_k2", new SortedSetEntry[]
        {
            new("x", 10), new("y", 20)
        });

        var result = await _db.ExecuteAsync("ZMPOP", "2", "zmpop_k1_empty", "zmpop_k2", "MIN");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("zmpop_k2", (string)arr[0]!);
    }

    [Fact]
    public async Task ZMPop_removes_key_when_empty()
    {
        await _db.SortedSetAddAsync("zmpop_del", new SortedSetEntry[]
        {
            new("only", 1)
        });

        await _db.ExecuteAsync("ZMPOP", "1", "zmpop_del", "MIN");
        Assert.False(await _db.KeyExistsAsync("zmpop_del"));
    }

    // =====================================================================
    // BZMPOP
    // Ref: https://redis.io/docs/latest/commands/bzmpop/
    // =====================================================================

    [Fact]
    public async Task BZMPop_returns_immediately_with_elements()
    {
        await _db.SortedSetAddAsync("bzmpop1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var result = await _db.ExecuteAsync("BZMPOP", "1", "1", "bzmpop1", "MIN");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("bzmpop1", (string)arr[0]!);
        var elements = (RedisResult[])arr[1]!;
        var entry = (RedisResult[])elements[0]!;
        Assert.Equal("a", (string)entry[0]!);
    }

    [Fact]
    public async Task BZMPop_returns_nil_on_timeout()
    {
        var result = await _db.ExecuteAsync("BZMPOP", "1", "1", "bzmpop_empty", "MIN");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task BZMPop_max_pops_highest()
    {
        await _db.SortedSetAddAsync("bzmpop2", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var result = await _db.ExecuteAsync("BZMPOP", "1", "1", "bzmpop2", "MAX");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        var elements = (RedisResult[])arr[1]!;
        var entry = (RedisResult[])elements[0]!;
        Assert.Equal("c", (string)entry[0]!);
    }

    [Fact]
    public async Task BZMPop_blocks_and_returns_when_element_added()
    {
        var mux = await _fixture.GetMultiplexerAsync();
        var endpoint = mux.GetEndPoints()[0];
        var opts = new ConfigurationOptions { AbortOnConnectFail = false };
        opts.EndPoints.Add(endpoint);
        await using var mux2 = await ConnectionMultiplexer.ConnectAsync(opts);
        var db2 = mux2.GetDatabase();

        var popTask = Task.Run(async () =>
            await db2.ExecuteAsync("BZMPOP", "3", "1", "bzmpop_block", "MIN"));

        await Task.Delay(300);
        await _db.SortedSetAddAsync("bzmpop_block", "arrived", 42);

        var result = await popTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("bzmpop_block", (string)arr[0]!);
        var elements = (RedisResult[])arr[1]!;
        var entry = (RedisResult[])elements[0]!;
        Assert.Equal("arrived", (string)entry[0]!);
    }
}
