using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ListCommandGapTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public ListCommandGapTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ═══════════════════════════════════════════════════════════════
    // LMOVE tests
    // Ref: https://redis.io/docs/latest/commands/lmove/
    //   "Atomically returns and removes the first/last element (head/tail
    //    depending on the wherefrom argument) of the list stored at source,
    //    and pushes the element at the first/last element (head/tail
    //    depending on the whereto argument) of the list stored at destination."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task LMove_left_to_right_moves_element()
    {
        await _db.ListRightPushAsync("lmove_src1", new RedisValue[] { "a", "b", "c" });
        var moved = await _db.ListMoveAsync("lmove_src1", "lmove_dst1", ListSide.Left, ListSide.Right);
        Assert.Equal("a", moved.ToString());

        // Source should have [b, c]
        var srcItems = await _db.ListRangeAsync("lmove_src1");
        Assert.Equal(2, srcItems.Length);
        Assert.Equal("b", srcItems[0].ToString());
        Assert.Equal("c", srcItems[1].ToString());

        // Dest should have [a]
        var dstItems = await _db.ListRangeAsync("lmove_dst1");
        Assert.Single(dstItems);
        Assert.Equal("a", dstItems[0].ToString());
    }

    [Fact]
    public async Task LMove_right_to_left_moves_element()
    {
        await _db.ListRightPushAsync("lmove_src2", new RedisValue[] { "x", "y", "z" });
        await _db.ListRightPushAsync("lmove_dst2", new RedisValue[] { "existing" });
        var moved = await _db.ListMoveAsync("lmove_src2", "lmove_dst2", ListSide.Right, ListSide.Left);
        Assert.Equal("z", moved.ToString());

        // Source should have [x, y]
        var srcItems = await _db.ListRangeAsync("lmove_src2");
        Assert.Equal(2, srcItems.Length);
        Assert.Equal("x", srcItems[0].ToString());
        Assert.Equal("y", srcItems[1].ToString());

        // Dest should have [z, existing]
        var dstItems = await _db.ListRangeAsync("lmove_dst2");
        Assert.Equal(2, dstItems.Length);
        Assert.Equal("z", dstItems[0].ToString());
        Assert.Equal("existing", dstItems[1].ToString());
    }

    [Fact]
    public async Task LMove_nonexistent_source_returns_null()
    {
        var result = await _db.ListMoveAsync("lmove_ghost", "lmove_dst_ghost", ListSide.Left, ListSide.Right);
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task LMove_removes_source_key_when_last_element_moved()
    {
        await _db.ListRightPushAsync("lmove_single", "only");
        await _db.ListMoveAsync("lmove_single", "lmove_single_dst", ListSide.Left, ListSide.Right);
        Assert.False(await _db.KeyExistsAsync("lmove_single"));
    }

    // ═══════════════════════════════════════════════════════════════
    // LMOVE same key (rotation) tests
    // Ref: https://redis.io/docs/latest/commands/lmove/
    //   "If source and destination are the same, the command is
    //    equivalent to removing one element from one end of a list
    //    and pushing it to the other end."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task LMove_same_key_rotates_right_to_left()
    {
        await _db.ListRightPushAsync("lmove_rotate1", new RedisValue[] { "a", "b", "c" });
        var moved = await _db.ListMoveAsync("lmove_rotate1", "lmove_rotate1", ListSide.Right, ListSide.Left);
        Assert.Equal("c", moved.ToString());

        // After rotation: [c, a, b]
        var items = await _db.ListRangeAsync("lmove_rotate1");
        Assert.Equal(3, items.Length);
        Assert.Equal("c", items[0].ToString());
        Assert.Equal("a", items[1].ToString());
        Assert.Equal("b", items[2].ToString());
    }

    [Fact]
    public async Task LMove_same_key_rotates_left_to_right()
    {
        await _db.ListRightPushAsync("lmove_rotate2", new RedisValue[] { "a", "b", "c" });
        var moved = await _db.ListMoveAsync("lmove_rotate2", "lmove_rotate2", ListSide.Left, ListSide.Right);
        Assert.Equal("a", moved.ToString());

        // After rotation: [b, c, a]
        var items = await _db.ListRangeAsync("lmove_rotate2");
        Assert.Equal(3, items.Length);
        Assert.Equal("b", items[0].ToString());
        Assert.Equal("c", items[1].ToString());
        Assert.Equal("a", items[2].ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // LPOS tests
    // Ref: https://redis.io/docs/latest/commands/lpos/
    //   "Returns the index of matching elements inside a Redis list."
    //   "By default, when no options are given, it will scan the list
    //    from head to tail, looking for the first match of 'element'."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task LPos_returns_index_of_element()
    {
        await _db.ListRightPushAsync("lpos_key1", new RedisValue[] { "a", "b", "c", "d" });
        var pos = await _db.ListPositionAsync("lpos_key1", "c");
        Assert.Equal(2, pos);
    }

    [Fact]
    public async Task LPos_returns_first_match_by_default()
    {
        await _db.ListRightPushAsync("lpos_dup", new RedisValue[] { "a", "b", "a", "c", "a" });
        var pos = await _db.ListPositionAsync("lpos_dup", "a");
        Assert.Equal(0, pos);
    }

    // ═══════════════════════════════════════════════════════════════
    // LPOS with RANK tests
    // Ref: https://redis.io/docs/latest/commands/lpos/
    //   "RANK: specifies the 'rank' of the first element to return,
    //    in case there are multiple matches. A rank of 1 means to return
    //    the first match, 2 to return the second match, and so forth."
    //   "Negative rank reverses the search direction."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task LPos_with_rank_skips_matches()
    {
        await _db.ListRightPushAsync("lpos_rank", new RedisValue[] { "a", "b", "a", "c", "a" });
        var pos = await _db.ListPositionAsync("lpos_rank", "a", rank: 2);
        Assert.Equal(2, pos);
    }

    [Fact]
    public async Task LPos_with_rank_3_returns_third_match()
    {
        await _db.ListRightPushAsync("lpos_rank3", new RedisValue[] { "a", "b", "a", "c", "a" });
        var pos = await _db.ListPositionAsync("lpos_rank3", "a", rank: 3);
        Assert.Equal(4, pos);
    }

    [Fact]
    public async Task LPos_with_negative_rank_searches_from_tail()
    {
        await _db.ListRightPushAsync("lpos_negrank", new RedisValue[] { "a", "b", "a", "c", "a" });
        var pos = await _db.ListPositionAsync("lpos_negrank", "a", rank: -1);
        Assert.Equal(4, pos);
    }

    // ═══════════════════════════════════════════════════════════════
    // LPOS with COUNT tests
    // Ref: https://redis.io/docs/latest/commands/lpos/
    //   "COUNT: By default a single match is returned. When COUNT is given,
    //    up to count matches are returned. COUNT 0 means all matches."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task LPos_with_count_returns_multiple_positions()
    {
        await _db.ListRightPushAsync("lpos_count", new RedisValue[] { "a", "b", "a", "c", "a" });
        var positions = await _db.ListPositionsAsync("lpos_count", "a", count: 2);
        Assert.Equal(2, positions.Length);
        Assert.Equal(0, positions[0]);
        Assert.Equal(2, positions[1]);
    }

    [Fact]
    public async Task LPos_with_count_zero_returns_all_matches()
    {
        await _db.ListRightPushAsync("lpos_countall", new RedisValue[] { "a", "b", "a", "c", "a" });
        var positions = await _db.ListPositionsAsync("lpos_countall", "a", count: 0);
        Assert.Equal(3, positions.Length);
        Assert.Equal(0, positions[0]);
        Assert.Equal(2, positions[1]);
        Assert.Equal(4, positions[2]);
    }

    // ═══════════════════════════════════════════════════════════════
    // LPOS nonexistent value tests
    // Ref: https://redis.io/docs/latest/commands/lpos/
    //   "The command returns the integer representing the matching element,
    //    or null if there is no match."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task LPos_nonexistent_value_returns_null()
    {
        await _db.ListRightPushAsync("lpos_novalue", new RedisValue[] { "a", "b", "c" });
        var pos = await _db.ListPositionAsync("lpos_novalue", "z");
        Assert.Equal(-1, pos);
    }

    [Fact]
    public async Task LPos_nonexistent_key_returns_null()
    {
        var pos = await _db.ListPositionAsync("lpos_nokey", "a");
        Assert.Equal(-1, pos);
    }

    // ═══════════════════════════════════════════════════════════════
    // BLMOVE tests
    // Ref: https://redis.io/docs/latest/commands/blmove/
    //   "BLMOVE is the blocking variant of LMOVE. When source contains
    //    elements, this command behaves exactly like LMOVE."
    //   "When source is empty, Redis will block the connection until
    //    another client pushes to it or until timeout is reached."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task BLMove_returns_immediately_when_source_has_elements()
    {
        await _db.ListRightPushAsync("blmove_src", new RedisValue[] { "a", "b", "c" });
        var result = await _db.ExecuteAsync("BLMOVE", "blmove_src", "blmove_dst", "LEFT", "RIGHT", "1");
        Assert.False(result.IsNull);
        Assert.Equal("a", (string)result!);

        // Source should have [b, c]
        var srcItems = await _db.ListRangeAsync("blmove_src");
        Assert.Equal(2, srcItems.Length);
        Assert.Equal("b", srcItems[0].ToString());
        Assert.Equal("c", srcItems[1].ToString());

        // Dest should have [a]
        var dstItems = await _db.ListRangeAsync("blmove_dst");
        Assert.Single(dstItems);
        Assert.Equal("a", dstItems[0].ToString());
    }

    [Fact]
    public async Task BLMove_returns_nil_on_timeout()
    {
        var result = await _db.ExecuteAsync("BLMOVE", "blmove_empty", "blmove_dst2", "LEFT", "RIGHT", "1");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task BLMove_right_to_left_moves_tail_to_head()
    {
        await _db.ListRightPushAsync("blmove_rl_src", new RedisValue[] { "x", "y", "z" });
        var result = await _db.ExecuteAsync("BLMOVE", "blmove_rl_src", "blmove_rl_dst", "RIGHT", "LEFT", "1");
        Assert.Equal("z", (string)result!);

        var dstItems = await _db.ListRangeAsync("blmove_rl_dst");
        Assert.Single(dstItems);
        Assert.Equal("z", dstItems[0].ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // LSET tests
    // Ref: https://redis.io/docs/latest/commands/lset/
    //   "Sets the list element at index to element."
    //   "An error is returned for out of range indexes."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task LSet_sets_element_at_index()
    {
        await _db.ListRightPushAsync("lset_key", new RedisValue[] { "a", "b", "c" });
        await _db.ListSetByIndexAsync("lset_key", 1, "B");
        var items = await _db.ListRangeAsync("lset_key");
        Assert.Equal(3, items.Length);
        Assert.Equal("a", items[0].ToString());
        Assert.Equal("B", items[1].ToString());
        Assert.Equal("c", items[2].ToString());
    }

    [Fact]
    public async Task LSet_sets_first_element()
    {
        await _db.ListRightPushAsync("lset_first", new RedisValue[] { "a", "b", "c" });
        await _db.ListSetByIndexAsync("lset_first", 0, "X");
        var first = await _db.ListGetByIndexAsync("lset_first", 0);
        Assert.Equal("X", first.ToString());
    }

    [Fact]
    public async Task LSet_sets_last_element_with_negative_index()
    {
        await _db.ListRightPushAsync("lset_neg", new RedisValue[] { "a", "b", "c" });
        await _db.ListSetByIndexAsync("lset_neg", -1, "Z");
        var last = await _db.ListGetByIndexAsync("lset_neg", -1);
        Assert.Equal("Z", last.ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // LSET out of range tests
    // Ref: https://redis.io/docs/latest/commands/lset/
    //   "An error is returned for out of range indexes."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task LSet_out_of_range_throws_error()
    {
        await _db.ListRightPushAsync("lset_oor", new RedisValue[] { "a", "b", "c" });
        await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.ListSetByIndexAsync("lset_oor", 10, "X"));
    }

    [Fact]
    public async Task LSet_negative_out_of_range_throws_error()
    {
        await _db.ListRightPushAsync("lset_noor", new RedisValue[] { "a", "b", "c" });
        await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.ListSetByIndexAsync("lset_noor", -10, "X"));
    }

    [Fact]
    public async Task LSet_on_nonexistent_key_throws_error()
    {
        await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.ListSetByIndexAsync("lset_missing", 0, "X"));
    }
}
