using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class MiscCommandTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public MiscCommandTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ═══════════════════════════════════════════════════════════════
    // LMPOP tests
    // Ref: https://redis.io/docs/latest/commands/lmpop/
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task LMPOP_pops_from_left_of_first_nonempty_list()
    {
        await _db.ListRightPushAsync("lmpop1", new RedisValue[] { "a", "b", "c" });
        var result = await _db.ExecuteAsync("LMPOP", "1", "lmpop1", "LEFT");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("lmpop1", (string)arr[0]!);
        var elements = (RedisResult[])arr[1]!;
        Assert.Single(elements);
        Assert.Equal("a", (string)elements[0]!);
    }

    [Fact]
    public async Task LMPOP_pops_from_right_with_count()
    {
        await _db.ListRightPushAsync("lmpop2", new RedisValue[] { "a", "b", "c", "d" });
        var result = await _db.ExecuteAsync("LMPOP", "1", "lmpop2", "RIGHT", "COUNT", "2");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("lmpop2", (string)arr[0]!);
        var elements = (RedisResult[])arr[1]!;
        Assert.Equal(2, elements.Length);
        Assert.Equal("d", (string)elements[0]!);
        Assert.Equal("c", (string)elements[1]!);

        // Verify remaining elements
        var remaining = await _db.ListRangeAsync("lmpop2");
        Assert.Equal(2, remaining.Length);
        Assert.Equal("a", (string)remaining[0]!);
        Assert.Equal("b", (string)remaining[1]!);
    }

    [Fact]
    public async Task LMPOP_returns_nil_when_all_keys_empty()
    {
        var result = await _db.ExecuteAsync("LMPOP", "1", "lmpop_nonexist", "LEFT");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task LMPOP_checks_multiple_keys_returns_first_nonempty()
    {
        // First key empty, second has data
        await _db.ListRightPushAsync("lmpop_multi2", new RedisValue[] { "x", "y" });
        var result = await _db.ExecuteAsync("LMPOP", "2", "lmpop_multi1", "lmpop_multi2", "LEFT");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal("lmpop_multi2", (string)arr[0]!);
    }

    // ═══════════════════════════════════════════════════════════════
    // BRPOPLPUSH tests
    // Ref: https://redis.io/docs/latest/commands/brpoplpush/
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task BRPOPLPUSH_pops_from_source_tail_and_pushes_to_dest_head()
    {
        await _db.ListRightPushAsync("brpl_src", new RedisValue[] { "a", "b", "c" });
        var result = await _db.ExecuteAsync("BRPOPLPUSH", "brpl_src", "brpl_dst", "1");
        Assert.False(result.IsNull);
        Assert.Equal("c", (string)result!);

        // Verify source has remaining elements
        var srcItems = await _db.ListRangeAsync("brpl_src");
        Assert.Equal(2, srcItems.Length);
        Assert.Equal("a", (string)srcItems[0]!);
        Assert.Equal("b", (string)srcItems[1]!);

        // Verify destination has the popped element at the head
        var dstItems = await _db.ListRangeAsync("brpl_dst");
        Assert.Single(dstItems);
        Assert.Equal("c", (string)dstItems[0]!);
    }

    [Fact]
    public async Task BRPOPLPUSH_returns_nil_on_timeout()
    {
        var result = await _db.ExecuteAsync("BRPOPLPUSH", "brpl_empty", "brpl_dst2", "1");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task BRPOPLPUSH_same_source_and_dest_rotates_list()
    {
        await _db.ListRightPushAsync("brpl_rotate", new RedisValue[] { "a", "b", "c" });
        var result = await _db.ExecuteAsync("BRPOPLPUSH", "brpl_rotate", "brpl_rotate", "1");
        Assert.False(result.IsNull);
        Assert.Equal("c", (string)result!);

        // After rotation: c, a, b
        var items = await _db.ListRangeAsync("brpl_rotate");
        Assert.Equal(3, items.Length);
        Assert.Equal("c", (string)items[0]!);
        Assert.Equal("a", (string)items[1]!);
        Assert.Equal("b", (string)items[2]!);
    }

    // ═══════════════════════════════════════════════════════════════
    // SPUBLISH tests
    // Ref: https://redis.io/docs/latest/commands/spublish/
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task SPUBLISH_returns_number_of_receivers()
    {
        // Without subscribers, should return 0
        var result = await _db.ExecuteAsync("SPUBLISH", "shard_channel", "hello");
        Assert.Equal(0, (long)result);
    }

    [Fact]
    public async Task SPUBLISH_returns_zero_in_standalone_mode()
    {
        // Ref: https://redis.io/docs/latest/commands/spublish/
        //   In standalone mode, SPUBLISH always returns 0 because sharded pub/sub
        //   only works in cluster mode.
        var result = await _db.ExecuteAsync("SPUBLISH", "spub_test", "hello_shard");
        Assert.Equal(0, (long)result);
    }

    // ═══════════════════════════════════════════════════════════════
    // CLIENT SETINFO tests
    // Ref: https://redis.io/docs/latest/commands/client-setinfo/
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task CLIENT_SETINFO_lib_name_returns_ok()
    {
        var result = await _db.ExecuteAsync("CLIENT", "SETINFO", "lib-name", "mylib");
        Assert.Equal("OK", (string)result!);
    }

    [Fact]
    public async Task CLIENT_SETINFO_lib_ver_returns_ok()
    {
        var result = await _db.ExecuteAsync("CLIENT", "SETINFO", "lib-ver", "1.2.3");
        Assert.Equal("OK", (string)result!);
    }

    [Fact]
    public async Task CLIENT_SETINFO_unknown_attr_returns_error()
    {
        await Assert.ThrowsAsync<RedisServerException>(async () =>
            await _db.ExecuteAsync("CLIENT", "SETINFO", "unknown-attr", "value"));
    }

    // ═══════════════════════════════════════════════════════════════
    // MOVE tests
    // Ref: https://redis.io/docs/latest/commands/move/
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task MOVE_moves_key_to_another_database()
    {
        await _db.StringSetAsync("move_key", "move_value");
        var moved = await _db.KeyMoveAsync("move_key", 1);
        Assert.True(moved);

        // Key should no longer exist in source db
        Assert.False(await _db.KeyExistsAsync("move_key"));

        // Key should exist in target db
        var db1 = (await _fixture.GetMultiplexerAsync()).GetDatabase(1);
        var val = await db1.StringGetAsync("move_key");
        Assert.Equal("move_value", (string)val!);

        // Cleanup
        await db1.KeyDeleteAsync("move_key");
    }

    [Fact]
    public async Task MOVE_returns_false_when_key_missing()
    {
        var moved = await _db.KeyMoveAsync("move_nonexist", 1);
        Assert.False(moved);
    }

    [Fact]
    public async Task MOVE_returns_false_when_target_already_has_key()
    {
        var db1 = (await _fixture.GetMultiplexerAsync()).GetDatabase(1);
        await _db.StringSetAsync("move_dup", "src_val");
        await db1.StringSetAsync("move_dup", "dst_val");

        var moved = await _db.KeyMoveAsync("move_dup", 1);
        Assert.False(moved);

        // Source should still have original value
        var srcVal = await _db.StringGetAsync("move_dup");
        Assert.Equal("src_val", (string)srcVal!);

        // Cleanup
        await db1.KeyDeleteAsync("move_dup");
    }

    // ═══════════════════════════════════════════════════════════════
    // SWAPDB tests
    // Ref: https://redis.io/docs/latest/commands/swapdb/
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task SWAPDB_swaps_two_databases()
    {
        var mux = await _fixture.GetMultiplexerAsync();
        var db0 = mux.GetDatabase(0);
        var db1 = mux.GetDatabase(1);

        await db0.StringSetAsync("swap_key0", "val0");
        await db1.StringSetAsync("swap_key1", "val1");

        var result = await db0.ExecuteAsync("SWAPDB", "0", "1");
        Assert.Equal("OK", (string)result!);

        // After swap: db0 should have swap_key1, db1 should have swap_key0
        Assert.Equal("val1", (string)(await db0.StringGetAsync("swap_key1"))!);
        Assert.Equal("val0", (string)(await db1.StringGetAsync("swap_key0"))!);

        // swap_key0 should NOT be in db0, swap_key1 should NOT be in db1
        Assert.False(await db0.KeyExistsAsync("swap_key0"));
        Assert.False(await db1.KeyExistsAsync("swap_key1"));

        // Swap back to clean up
        await db0.ExecuteAsync("SWAPDB", "0", "1");
        await db0.KeyDeleteAsync("swap_key0");
        await db1.KeyDeleteAsync("swap_key1");
    }

    [Fact]
    public async Task SWAPDB_same_index_is_noop()
    {
        await _db.StringSetAsync("swap_same", "value");
        var result = await _db.ExecuteAsync("SWAPDB", "0", "0");
        Assert.Equal("OK", (string)result!);
        Assert.Equal("value", (string)(await _db.StringGetAsync("swap_same"))!);
    }

    // ═══════════════════════════════════════════════════════════════
    // SCRIPT KILL tests
    // Ref: https://redis.io/docs/latest/commands/script-kill/
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task SCRIPT_KILL_returns_ok()
    {
        // In emulator, SCRIPT KILL is a no-op. For real Redis, it returns
        // NOTBUSY if no script is running - accept both outcomes.
        try
        {
            var result = await _db.ExecuteAsync("SCRIPT", "KILL");
            Assert.Equal("OK", (string)result!);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("NOTBUSY"))
        {
            // Real Redis returns NOTBUSY when no script is running - acceptable
        }
    }

    [Fact]
    public async Task SCRIPT_EXISTS_returns_zero_for_unknown_sha()
    {
        var result = await _db.ExecuteAsync("SCRIPT", "EXISTS", "0000000000000000000000000000000000000000");
        var arr = (RedisResult[])result!;
        Assert.Single(arr);
        Assert.Equal(0, (long)arr[0]!);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEORADIUS tests
    // Ref: https://redis.io/docs/latest/commands/georadius/
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GEORADIUS_returns_members_within_radius()
    {
        await _db.GeoAddAsync("geo_radius", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(2.349014, 48.864716, "Paris"),
        });

        // Search within 200km of Palermo's location - should find Palermo and Catania but not Paris
        var results = await _db.GeoRadiusAsync("geo_radius", 13.361389, 38.115556, 200, GeoUnit.Kilometers);
        Assert.NotNull(results);
        var names = results.Select(r => r.Member.ToString()).OrderBy(n => n).ToArray();
        Assert.Contains("Palermo", names);
        Assert.Contains("Catania", names);
        Assert.DoesNotContain("Paris", names);
    }

    [Fact]
    public async Task GEORADIUS_with_withdist_returns_distances()
    {
        await _db.GeoAddAsync("geo_radius_dist", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
        });

        var results = await _db.GeoRadiusAsync("geo_radius_dist", 13.361389, 38.115556, 200,
            GeoUnit.Kilometers, options: GeoRadiusOptions.WithDistance);
        Assert.NotNull(results);
        Assert.True(results.Length >= 1);
        // Palermo should have ~0 distance
        var palermo = results.First(r => r.Member == "Palermo");
        Assert.InRange(palermo.Distance!.Value, 0, 1);
    }

    [Fact]
    public async Task GEORADIUS_with_count_limits_results()
    {
        await _db.GeoAddAsync("geo_radius_count", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(14.26812, 40.85216, "Naples"),
        });

        var results = await _db.GeoRadiusAsync("geo_radius_count", 13.361389, 38.115556, 500,
            GeoUnit.Kilometers, count: 2, order: Order.Ascending);
        Assert.NotNull(results);
        Assert.Equal(2, results.Length);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEORADIUSBYMEMBER tests
    // Ref: https://redis.io/docs/latest/commands/georadiusbymember/
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GEORADIUSBYMEMBER_returns_nearby_members()
    {
        await _db.GeoAddAsync("geo_member", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(2.349014, 48.864716, "Paris"),
        });

        var result = await _db.ExecuteAsync("GEORADIUSBYMEMBER", "geo_member", "Palermo", "200", "km");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        var members = arr.Select(r => (string)r!).ToArray();
        Assert.Contains("Palermo", members);
        Assert.Contains("Catania", members);
        Assert.DoesNotContain("Paris", members);
    }

    [Fact]
    public async Task GEORADIUSBYMEMBER_with_count_and_asc_limits_results()
    {
        await _db.GeoAddAsync("geo_member_count", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(14.26812, 40.85216, "Naples"),
        });

        var result = await _db.ExecuteAsync("GEORADIUSBYMEMBER", "geo_member_count", "Palermo", "500", "km", "COUNT", "2", "ASC");
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;
        Assert.Equal(2, arr.Length);
        // First should be Palermo (distance 0)
        Assert.Equal("Palermo", (string)arr[0]!);
    }

    [Fact]
    public async Task GEORADIUSBYMEMBER_on_empty_key_returns_empty()
    {
        var result = await _db.ExecuteAsync("GEORADIUSBYMEMBER", "geo_nonexist", "Member", "100", "km");
        var arr = (RedisResult[])result!;
        Assert.Empty(arr);
    }
}
