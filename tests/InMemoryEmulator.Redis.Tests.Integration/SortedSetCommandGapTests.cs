using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class SortedSetCommandGapTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public SortedSetCommandGapTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // =====================================================================
    // ZRANGESTORE
    // Ref: https://redis.io/docs/latest/commands/zrangestore/
    //   "Stores a range of members from sorted set into another key."
    // =====================================================================

    [Fact]
    public async Task ZRangeStore_stores_full_range()
    {
        await _db.SortedSetAddAsync("zrs_src", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.ExecuteAsync("ZRANGESTORE", "zrs_dest", "zrs_src", "0", "-1");
        Assert.Equal(5, (long)result);

        var members = await _db.SortedSetRangeByRankWithScoresAsync("zrs_dest");
        Assert.Equal(5, members.Length);
        Assert.Equal("a", members[0].Element.ToString());
        Assert.Equal(1.0, members[0].Score);
        Assert.Equal("e", members[4].Element.ToString());
        Assert.Equal(5.0, members[4].Score);
    }

    [Fact]
    public async Task ZRangeStore_stores_partial_range()
    {
        await _db.SortedSetAddAsync("zrs_part_src", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.ExecuteAsync("ZRANGESTORE", "zrs_part_dest", "zrs_part_src", "1", "3");
        Assert.Equal(3, (long)result);

        var members = await _db.SortedSetRangeByRankAsync("zrs_part_dest");
        Assert.Equal(3, members.Length);
        Assert.Equal("b", members[0].ToString());
        Assert.Equal("c", members[1].ToString());
        Assert.Equal("d", members[2].ToString());
    }

    [Fact]
    public async Task ZRangeStore_nonexistent_source_returns_zero()
    {
        var result = await _db.ExecuteAsync("ZRANGESTORE", "zrs_nokey_dest", "zrs_nokey_src", "0", "-1");
        Assert.Equal(0, (long)result);
        Assert.False(await _db.KeyExistsAsync("zrs_nokey_dest"));
    }

    // =====================================================================
    // ZDIFFSTORE
    // Ref: https://redis.io/docs/latest/commands/zdiffstore/
    //   "Computes the difference between the first and all successive
    //    sorted sets and stores the result in destination."
    // =====================================================================

    [Fact]
    public async Task ZDiffStore_stores_difference()
    {
        await _db.SortedSetAddAsync("zdst_k1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });
        await _db.SortedSetAddAsync("zdst_k2", new SortedSetEntry[]
        {
            new("b", 5), new("d", 4)
        });

        var count = await _db.SortedSetCombineAndStoreAsync(SetOperation.Difference, "zdst_dest", new RedisKey[] { "zdst_k1", "zdst_k2" });
        Assert.Equal(2, count);

        var members = await _db.SortedSetRangeByRankWithScoresAsync("zdst_dest");
        Assert.Equal(2, members.Length);
        Assert.Equal("a", members[0].Element.ToString());
        Assert.Equal(1.0, members[0].Score);
        Assert.Equal("c", members[1].Element.ToString());
        Assert.Equal(3.0, members[1].Score);
    }

    [Fact]
    public async Task ZDiffStore_result_count_matches_stored_members()
    {
        await _db.SortedSetAddAsync("zdst_rc_k1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4)
        });
        await _db.SortedSetAddAsync("zdst_rc_k2", new SortedSetEntry[]
        {
            new("a", 10), new("c", 30)
        });

        var count = await _db.SortedSetCombineAndStoreAsync(SetOperation.Difference, "zdst_rc_dest", new RedisKey[] { "zdst_rc_k1", "zdst_rc_k2" });
        Assert.Equal(2, count);

        var stored = await _db.SortedSetLengthAsync("zdst_rc_dest");
        Assert.Equal(count, stored);
    }

    [Fact]
    public async Task ZDiffStore_empty_diff_removes_destination()
    {
        await _db.SortedSetAddAsync("zdst_emp_dest", "pre_existing", 1);
        await _db.SortedSetAddAsync("zdst_emp_k1", new SortedSetEntry[] { new("a", 1) });
        await _db.SortedSetAddAsync("zdst_emp_k2", new SortedSetEntry[] { new("a", 2) });

        var count = await _db.SortedSetCombineAndStoreAsync(SetOperation.Difference, "zdst_emp_dest", new RedisKey[] { "zdst_emp_k1", "zdst_emp_k2" });
        Assert.Equal(0, count);
        Assert.False(await _db.KeyExistsAsync("zdst_emp_dest"));
    }

    // =====================================================================
    // ZSCAN
    // Ref: https://redis.io/docs/latest/commands/zscan/
    //   "Iterates elements of Sorted Set types and their associated scores."
    // =====================================================================

    [Fact]
    public async Task ZScan_iterates_all_members_with_scores()
    {
        var expected = new Dictionary<string, double>();
        for (int i = 0; i < 20; i++)
        {
            expected[$"member_{i:D3}"] = i * 1.5;
        }
        await _db.SortedSetAddAsync("zscan_k1",
            expected.Select(kv => new SortedSetEntry(kv.Key, kv.Value)).ToArray());

        var scanned = new Dictionary<string, double>();
        await foreach (var entry in _db.SortedSetScanAsync("zscan_k1", "*", pageSize: 5))
        {
            scanned[entry.Element.ToString()] = entry.Score;
        }

        Assert.Equal(expected.Count, scanned.Count);
        foreach (var kv in expected)
        {
            Assert.True(scanned.ContainsKey(kv.Key));
            Assert.Equal(kv.Value, scanned[kv.Key]);
        }
    }

    [Fact]
    public async Task ZScan_with_pattern_filters_members()
    {
        await _db.SortedSetAddAsync("zscan_pat", new SortedSetEntry[]
        {
            new("user:1", 10), new("user:2", 20), new("user:3", 30),
            new("item:1", 40), new("item:2", 50)
        });

        var scanned = new List<SortedSetEntry>();
        await foreach (var entry in _db.SortedSetScanAsync("zscan_pat", "user:*", pageSize: 10))
        {
            scanned.Add(entry);
        }

        Assert.Equal(3, scanned.Count);
        Assert.All(scanned, e => Assert.StartsWith("user:", e.Element.ToString()));
    }

    // =====================================================================
    // ZLEXCOUNT
    // Ref: https://redis.io/docs/latest/commands/zlexcount/
    //   "When all elements in a sorted set are inserted with the same
    //    score, in order to force lexicographic ordering, this command
    //    returns the number of elements in the sorted set at key with
    //    a value between min and max."
    // =====================================================================

    [Fact]
    public async Task ZLexCount_returns_count_in_lex_range()
    {
        await _db.SortedSetAddAsync("zlc_k1", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        var count = await _db.SortedSetLengthByValueAsync("zlc_k1", "b", "d");
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task ZLexCount_exclusive_bounds()
    {
        await _db.SortedSetAddAsync("zlc_excl", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        var count = await _db.SortedSetLengthByValueAsync("zlc_excl", "b", "d", Exclude.Both);
        Assert.Equal(1, count); // Only "c"
    }

    [Fact]
    public async Task ZLexCount_full_range()
    {
        await _db.SortedSetAddAsync("zlc_full", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0)
        });

        // Ref: https://redis.io/docs/latest/commands/zlexcount/
        //   "The special values + or - for start and stop have the special meaning
        //    of positively infinite and negatively infinite strings."
        // SortedSetLengthByValueAsync with default(RedisValue) sends bare "-" / "+"
        // as min/max, which Redis interprets as negative/positive infinity.
        var count = await _db.SortedSetLengthByValueAsync("zlc_full", default, default);
        Assert.Equal(3, count);
    }

    // =====================================================================
    // ZRANDMEMBER
    // Ref: https://redis.io/docs/latest/commands/zrandmember/
    //   "Return a random element from the sorted set value stored at key."
    // =====================================================================

    [Fact]
    public async Task ZRandMember_returns_random_member()
    {
        await _db.SortedSetAddAsync("zrm_k1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var result = await _db.ExecuteAsync("ZRANDMEMBER", "zrm_k1");
        Assert.False(result.IsNull);

        var member = (string)result!;
        Assert.Contains(member, new[] { "a", "b", "c" });

        // Sorted set should still have all members (not removed)
        Assert.Equal(3, await _db.SortedSetLengthAsync("zrm_k1"));
    }

    [Fact]
    public async Task ZRandMember_nonexistent_key_returns_null()
    {
        var result = await _db.ExecuteAsync("ZRANDMEMBER", "zrm_nokey");
        Assert.True(result.IsNull);
    }

    // =====================================================================
    // ZRANDMEMBER with count
    // Ref: https://redis.io/docs/latest/commands/zrandmember/
    //   "If count is positive, return an array of distinct elements.
    //    If count is negative, the behavior changes and the command is
    //    allowed to return the same element multiple times."
    // =====================================================================

    [Fact]
    public async Task ZRandMember_with_positive_count_returns_distinct()
    {
        await _db.SortedSetAddAsync("zrm_pos", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.ExecuteAsync("ZRANDMEMBER", "zrm_pos", "3");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);

        // All should be distinct
        var members = arr.Select(r => (string)r!).ToArray();
        Assert.Equal(members.Length, members.Distinct().Count());
    }

    [Fact]
    public async Task ZRandMember_with_negative_count_allows_repeats()
    {
        await _db.SortedSetAddAsync("zrm_neg", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2)
        });

        var result = await _db.ExecuteAsync("ZRANDMEMBER", "zrm_neg", "-5");
        var arr = (RedisResult[])result!;
        // Returns exactly |count| elements (may repeat)
        Assert.Equal(5, arr.Length);

        // All returned elements must be actual members
        foreach (var r in arr)
        {
            Assert.Contains((string)r!, new[] { "a", "b" });
        }
    }

    // =====================================================================
    // ZMSCORE
    // Ref: https://redis.io/docs/latest/commands/zmscore/
    //   "Returns the scores associated with the specified members in
    //    the sorted set stored at key."
    // =====================================================================

    [Fact]
    public async Task ZMScore_returns_scores_for_multiple_members()
    {
        await _db.SortedSetAddAsync("zms_k1", new SortedSetEntry[]
        {
            new("a", 1.5), new("b", 2.5), new("c", 3.5)
        });

        var scores = await _db.SortedSetScoresAsync("zms_k1", new RedisValue[] { "a", "nonexistent", "c" });
        Assert.Equal(3, scores.Length);
        Assert.Equal(1.5, scores[0]);
        Assert.Null(scores[1]); // nonexistent member returns null
        Assert.Equal(3.5, scores[2]);
    }

    [Fact]
    public async Task ZMScore_nonexistent_key_returns_all_nulls()
    {
        var scores = await _db.SortedSetScoresAsync("zms_nokey", new RedisValue[] { "a", "b" });
        Assert.Equal(2, scores.Length);
        Assert.Null(scores[0]);
        Assert.Null(scores[1]);
    }

    // =====================================================================
    // ZRANGEBYSCORE
    // Ref: https://redis.io/docs/latest/commands/zrangebyscore/
    //   "Returns all the elements in the sorted set at key with a score
    //    between min and max (including elements with score equal to
    //    min or max)."
    // =====================================================================

    [Fact]
    public async Task ZRangeByScore_returns_members_in_score_range()
    {
        await _db.SortedSetAddAsync("zrbs_k1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.SortedSetRangeByScoreAsync("zrbs_k1", 2, 4);
        Assert.Equal(3, result.Length);
        Assert.Equal("b", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
        Assert.Equal("d", result[2].ToString());
    }

    [Fact]
    public async Task ZRangeByScore_with_infinity()
    {
        await _db.SortedSetAddAsync("zrbs_inf", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var result = await _db.SortedSetRangeByScoreAsync("zrbs_inf",
            double.NegativeInfinity, double.PositiveInfinity);
        Assert.Equal(3, result.Length);
        Assert.Equal("a", result[0].ToString());
        Assert.Equal("c", result[2].ToString());
    }

    [Fact]
    public async Task ZRangeByScore_with_exclusive_bounds()
    {
        await _db.SortedSetAddAsync("zrbs_excl", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4)
        });

        var result = await _db.SortedSetRangeByScoreAsync("zrbs_excl", 1, 4, Exclude.Both);
        Assert.Equal(2, result.Length);
        Assert.Equal("b", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
    }

    // =====================================================================
    // ZREVRANGEBYSCORE
    // Ref: https://redis.io/docs/latest/commands/zrevrangebyscore/
    //   "Returns all the elements in the sorted set at key with a score
    //    between max and min, in descending order."
    // =====================================================================

    [Fact]
    public async Task ZRevRangeByScore_returns_members_descending()
    {
        await _db.SortedSetAddAsync("zrrbs_k1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.SortedSetRangeByScoreAsync("zrrbs_k1", 2, 4, order: Order.Descending);
        Assert.Equal(3, result.Length);
        Assert.Equal("d", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
        Assert.Equal("b", result[2].ToString());
    }

    // =====================================================================
    // ZRANGEBYLEX
    // Ref: https://redis.io/docs/latest/commands/zrangebylex/
    //   "When all the elements in a sorted set are inserted with the
    //    same score, this command returns all the elements in the sorted
    //    set at key with a value between min and max."
    // =====================================================================

    [Fact]
    public async Task ZRangeByLex_returns_members_in_lex_range()
    {
        await _db.SortedSetAddAsync("zrbl_k1", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        var result = await _db.SortedSetRangeByValueAsync("zrbl_k1", "b", "d");
        Assert.Equal(3, result.Length);
        Assert.Equal("b", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
        Assert.Equal("d", result[2].ToString());
    }

    [Fact]
    public async Task ZRangeByLex_exclusive_bounds()
    {
        await _db.SortedSetAddAsync("zrbl_excl", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        var result = await _db.SortedSetRangeByValueAsync("zrbl_excl", "a", "e", Exclude.Both);
        Assert.Equal(3, result.Length);
        Assert.Equal("b", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
        Assert.Equal("d", result[2].ToString());
    }

    // =====================================================================
    // ZREVRANGEBYLEX
    // Ref: https://redis.io/docs/latest/commands/zrevrangebylex/
    //   "Returns all the elements in the sorted set at key with a value
    //    between max and min, in reverse lexicographic ordering."
    // =====================================================================

    [Fact]
    public async Task ZRevRangeByLex_returns_members_in_reverse_lex_order()
    {
        await _db.SortedSetAddAsync("zrrbl_k1", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        var result = await _db.SortedSetRangeByValueAsync("zrrbl_k1", "d", "b", order: Order.Descending);
        Assert.Equal(3, result.Length);
        Assert.Equal("d", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
        Assert.Equal("b", result[2].ToString());
    }

    // =====================================================================
    // ZRANGESTORE with BYSCORE
    // Ref: https://redis.io/docs/latest/commands/zrangestore/
    //   "BYSCORE: Uses the score to filter and select elements."
    // =====================================================================

    [Fact]
    public async Task ZRangeStore_byscore_stores_score_range()
    {
        await _db.SortedSetAddAsync("zrss_src", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.ExecuteAsync("ZRANGESTORE", "zrss_dest", "zrss_src", "1", "5", "BYSCORE");
        Assert.Equal(5, (long)result);

        var members = await _db.SortedSetRangeByRankAsync("zrss_dest");
        Assert.Equal(5, members.Length);
    }

    [Fact]
    public async Task ZRangeStore_byscore_partial_range()
    {
        await _db.SortedSetAddAsync("zrss_part_src", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.ExecuteAsync("ZRANGESTORE", "zrss_part_dest", "zrss_part_src", "2", "4", "BYSCORE");
        Assert.Equal(3, (long)result);

        var members = await _db.SortedSetRangeByRankAsync("zrss_part_dest");
        Assert.Equal(3, members.Length);
        Assert.Equal("b", members[0].ToString());
        Assert.Equal("c", members[1].ToString());
        Assert.Equal("d", members[2].ToString());
    }

    // =====================================================================
    // ZRANGESTORE with BYLEX
    // Ref: https://redis.io/docs/latest/commands/zrangestore/
    //   "BYLEX: Uses the lexicographic order to filter and select elements."
    // =====================================================================

    [Fact]
    public async Task ZRangeStore_bylex_stores_lex_range()
    {
        await _db.SortedSetAddAsync("zrsl_src", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        var result = await _db.ExecuteAsync("ZRANGESTORE", "zrsl_dest", "zrsl_src", "[a", "[d", "BYLEX");
        Assert.Equal(4, (long)result);

        var members = await _db.SortedSetRangeByRankAsync("zrsl_dest");
        Assert.Equal(4, members.Length);
        Assert.Equal("a", members[0].ToString());
        Assert.Equal("b", members[1].ToString());
        Assert.Equal("c", members[2].ToString());
        Assert.Equal("d", members[3].ToString());
    }

    [Fact]
    public async Task ZRangeStore_bylex_exclusive_bounds()
    {
        await _db.SortedSetAddAsync("zrsl_excl_src", new SortedSetEntry[]
        {
            new("a", 0), new("b", 0), new("c", 0), new("d", 0), new("e", 0)
        });

        var result = await _db.ExecuteAsync("ZRANGESTORE", "zrsl_excl_dest", "zrsl_excl_src", "(a", "(e", "BYLEX");
        Assert.Equal(3, (long)result);

        var members = await _db.SortedSetRangeByRankAsync("zrsl_excl_dest");
        Assert.Equal(3, members.Length);
        Assert.Equal("b", members[0].ToString());
        Assert.Equal("c", members[1].ToString());
        Assert.Equal("d", members[2].ToString());
    }

    // =====================================================================
    // ZRANGEBYSCORE with LIMIT
    // Ref: https://redis.io/docs/latest/commands/zrangebyscore/
    //   "The optional LIMIT argument can be used to only get a range
    //    of the matching elements (similar to SELECT LIMIT offset, count
    //    in SQL)."
    // =====================================================================

    [Fact]
    public async Task ZRangeByScore_with_limit_skip_and_take()
    {
        await _db.SortedSetAddAsync("zrbsl_k1", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.SortedSetRangeByScoreAsync("zrbsl_k1", 1, 5, skip: 1, take: 2);
        Assert.Equal(2, result.Length);
        Assert.Equal("b", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
    }

    [Fact]
    public async Task ZRangeByScore_with_limit_skip_beyond_range()
    {
        await _db.SortedSetAddAsync("zrbsl_skip", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3)
        });

        var result = await _db.SortedSetRangeByScoreAsync("zrbsl_skip", 1, 3, skip: 10, take: 5);
        Assert.Empty(result);
    }

    [Fact]
    public async Task ZRangeByScore_descending_with_limit()
    {
        await _db.SortedSetAddAsync("zrbsl_desc", new SortedSetEntry[]
        {
            new("a", 1), new("b", 2), new("c", 3), new("d", 4), new("e", 5)
        });

        var result = await _db.SortedSetRangeByScoreAsync("zrbsl_desc", 1, 5,
            order: Order.Descending, skip: 1, take: 2);
        Assert.Equal(2, result.Length);
        Assert.Equal("d", result[0].ToString());
        Assert.Equal("c", result[1].ToString());
    }
}
