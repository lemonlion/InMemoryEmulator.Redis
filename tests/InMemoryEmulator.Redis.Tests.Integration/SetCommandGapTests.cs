using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class SetCommandGapTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public SetCommandGapTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // =====================================================================
    // SPOP — single member
    // Ref: https://redis.io/docs/latest/commands/spop/
    //   "Removes and returns one or more random members from the set"
    // =====================================================================

    [Fact]
    public async Task SPop_returns_and_removes_random_member()
    {
        await _db.SetAddAsync("spop_single", new RedisValue[] { "a", "b", "c" });

        var popped = await _db.SetPopAsync("spop_single");
        Assert.False(popped.IsNull);

        // The popped value must be one of the original members
        Assert.Contains(popped.ToString(), new[] { "a", "b", "c" });

        // Set should now have 2 members
        Assert.Equal(2, await _db.SetLengthAsync("spop_single"));

        // The popped member should no longer be in the set
        Assert.False(await _db.SetContainsAsync("spop_single", popped));
    }

    // =====================================================================
    // SPOP with count
    // Ref: https://redis.io/docs/latest/commands/spop/
    //   "When called with just the key argument, return a random element;
    //    with the additional count argument, return an array of count
    //    distinct elements."
    // =====================================================================

    [Fact]
    public async Task SPop_with_count_returns_and_removes_N_members()
    {
        await _db.SetAddAsync("spop_count", new RedisValue[] { "a", "b", "c", "d", "e" });

        var popped = await _db.SetPopAsync("spop_count", 3);
        Assert.Equal(3, popped.Length);

        // All popped members should be distinct
        Assert.Equal(popped.Length, popped.Select(v => v.ToString()).Distinct().Count());

        // Set should now have 2 members
        Assert.Equal(2, await _db.SetLengthAsync("spop_count"));

        // No popped member should remain in the set
        foreach (var member in popped)
        {
            Assert.False(await _db.SetContainsAsync("spop_count", member));
        }
    }

    [Fact]
    public async Task SPop_with_count_exceeding_set_size_returns_all()
    {
        await _db.SetAddAsync("spop_count_over", new RedisValue[] { "x", "y" });

        var popped = await _db.SetPopAsync("spop_count_over", 10);
        Assert.Equal(2, popped.Length);

        // Key should be removed when set is emptied
        Assert.False(await _db.KeyExistsAsync("spop_count_over"));
    }

    // =====================================================================
    // SRANDMEMBER — single member
    // Ref: https://redis.io/docs/latest/commands/srandmember/
    //   "Return a random element from the set value stored at key."
    // =====================================================================

    [Fact]
    public async Task SRandMember_returns_random_member_without_removing()
    {
        await _db.SetAddAsync("srand_single", new RedisValue[] { "a", "b", "c" });

        var member = await _db.SetRandomMemberAsync("srand_single");
        Assert.False(member.IsNull);
        Assert.Contains(member.ToString(), new[] { "a", "b", "c" });

        // Set should still have all 3 members (not removed)
        Assert.Equal(3, await _db.SetLengthAsync("srand_single"));
    }

    // =====================================================================
    // SRANDMEMBER with count
    // Ref: https://redis.io/docs/latest/commands/srandmember/
    //   "If count is positive, return an array of distinct elements.
    //    If count is negative, the behavior changes and the command is
    //    allowed to return the same element multiple times."
    // =====================================================================

    [Fact]
    public async Task SRandMembers_positive_count_returns_distinct_members()
    {
        await _db.SetAddAsync("srand_pos", new RedisValue[] { "a", "b", "c", "d", "e" });

        var members = await _db.SetRandomMembersAsync("srand_pos", 3);
        Assert.Equal(3, members.Length);

        // All members should be distinct
        Assert.Equal(members.Length, members.Select(v => v.ToString()).Distinct().Count());

        // All returned members should be actual set members
        var setMembers = await _db.SetMembersAsync("srand_pos");
        foreach (var m in members)
        {
            Assert.Contains(m, setMembers);
        }

        // Set should still have all 5 members
        Assert.Equal(5, await _db.SetLengthAsync("srand_pos"));
    }

    [Fact]
    public async Task SRandMembers_positive_count_exceeding_size_returns_all()
    {
        await _db.SetAddAsync("srand_pos_over", new RedisValue[] { "a", "b" });

        var members = await _db.SetRandomMembersAsync("srand_pos_over", 10);
        // When count > set size, Redis returns all elements (no repeats)
        Assert.Equal(2, members.Length);
    }

    [Fact]
    public async Task SRandMembers_negative_count_allows_repeats()
    {
        await _db.SetAddAsync("srand_neg", new RedisValue[] { "a", "b" });

        var members = await _db.SetRandomMembersAsync("srand_neg", -5);
        // Negative count: returns exactly |count| elements, may repeat
        Assert.Equal(5, members.Length);

        // All returned members should be actual set members
        var setMembers = await _db.SetMembersAsync("srand_neg");
        foreach (var m in members)
        {
            Assert.Contains(m, setMembers);
        }
    }

    // =====================================================================
    // SINTERCARD
    // Ref: https://redis.io/docs/latest/commands/sintercard/
    //   "Returns the cardinality of the intersection of the sets."
    // =====================================================================

    [Fact]
    public async Task SInterCard_returns_intersection_cardinality()
    {
        await _db.SetAddAsync("sic_k1", new RedisValue[] { "a", "b", "c", "d" });
        await _db.SetAddAsync("sic_k2", new RedisValue[] { "b", "c", "e" });

        var result = await _db.ExecuteAsync("SINTERCARD", "2", "sic_k1", "sic_k2");
        Assert.Equal(2, (long)result);
    }

    // =====================================================================
    // SINTERCARD with LIMIT
    // Ref: https://redis.io/docs/latest/commands/sintercard/
    //   "When given the optional LIMIT argument, if the intersection
    //    cardinality reaches limit partway through the computation,
    //    the algorithm will exit and yield limit as the cardinality."
    // =====================================================================

    [Fact]
    public async Task SInterCard_with_limit_caps_result()
    {
        await _db.SetAddAsync("sicl_k1", new RedisValue[] { "a", "b", "c", "d" });
        await _db.SetAddAsync("sicl_k2", new RedisValue[] { "a", "b", "c", "d" });

        var result = await _db.ExecuteAsync("SINTERCARD", "2", "sicl_k1", "sicl_k2", "LIMIT", "1");
        Assert.Equal(1, (long)result);
    }

    // =====================================================================
    // SMISMEMBER
    // Ref: https://redis.io/docs/latest/commands/smismember/
    //   "Returns whether each member is a member of the set stored at key."
    // =====================================================================

    [Fact]
    public async Task SMisMember_returns_membership_for_each_member()
    {
        await _db.SetAddAsync("smis_k1", new RedisValue[] { "a", "b", "c" });

        var result = await _db.ExecuteAsync("SMISMEMBER", "smis_k1", "a", "nonexistent", "c");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal(1, (long)arr[0]); // "a" is a member
        Assert.Equal(0, (long)arr[1]); // "nonexistent" is not
        Assert.Equal(1, (long)arr[2]); // "c" is a member
    }

    [Fact]
    public async Task SMisMember_nonexistent_key_returns_all_zeros()
    {
        var result = await _db.ExecuteAsync("SMISMEMBER", "smis_nokey", "a", "b");
        var arr = (RedisResult[])result!;
        Assert.Equal(2, arr.Length);
        Assert.Equal(0, (long)arr[0]);
        Assert.Equal(0, (long)arr[1]);
    }

    // =====================================================================
    // SSCAN
    // Ref: https://redis.io/docs/latest/commands/sscan/
    //   "Iterates elements of Sets types."
    // =====================================================================

    [Fact]
    public async Task SScan_iterates_all_members()
    {
        var expectedMembers = new HashSet<string>();
        for (int i = 0; i < 20; i++)
        {
            var member = $"member_{i:D3}";
            expectedMembers.Add(member);
        }
        await _db.SetAddAsync("sscan_k1", expectedMembers.Select(m => (RedisValue)m).ToArray());

        var scanned = new HashSet<string>();
        await foreach (var entry in _db.SetScanAsync("sscan_k1", "*", pageSize: 5))
        {
            scanned.Add(entry.ToString());
        }

        Assert.Equal(expectedMembers, scanned);
    }

    [Fact]
    public async Task SScan_with_pattern_filters_members()
    {
        await _db.SetAddAsync("sscan_pat", new RedisValue[]
        {
            "user:1", "user:2", "user:3", "item:1", "item:2"
        });

        var scanned = new List<string>();
        await foreach (var entry in _db.SetScanAsync("sscan_pat", "user:*", pageSize: 10))
        {
            scanned.Add(entry.ToString());
        }

        Assert.Equal(3, scanned.Count);
        Assert.All(scanned, s => Assert.StartsWith("user:", s));
    }

    // =====================================================================
    // SPOP on empty key
    // Ref: https://redis.io/docs/latest/commands/spop/
    //   "When called with just the key argument, the return value is a
    //    Bulk string reply: the removed member, or nil when key does
    //    not exist."
    // =====================================================================

    [Fact]
    public async Task SPop_on_empty_key_returns_null()
    {
        var result = await _db.SetPopAsync("spop_nokey");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task SPop_with_count_on_empty_key_returns_empty_array()
    {
        var result = await _db.SetPopAsync("spop_nokey_count", 5);
        Assert.Empty(result);
    }

    // =====================================================================
    // SRANDMEMBER on empty key
    // Ref: https://redis.io/docs/latest/commands/srandmember/
    //   "Bulk string reply: without the additional count argument, the
    //    command returns a random element, or nil when key does not exist."
    // =====================================================================

    [Fact]
    public async Task SRandMember_on_empty_key_returns_null()
    {
        var result = await _db.SetRandomMemberAsync("srand_nokey");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task SRandMembers_on_empty_key_returns_empty_array()
    {
        var result = await _db.SetRandomMembersAsync("srand_nokey_count", 5);
        Assert.Empty(result);
    }

    // =====================================================================
    // SINTERSTORE
    // Ref: https://redis.io/docs/latest/commands/sinterstore/
    //   "Store the intersection of multiple sets in a key."
    // =====================================================================

    [Fact]
    public async Task SInterStore_stores_intersection()
    {
        await _db.SetAddAsync("sist_k1", new RedisValue[] { "a", "b", "c", "d" });
        await _db.SetAddAsync("sist_k2", new RedisValue[] { "b", "c", "e" });

        var count = await _db.SetCombineAndStoreAsync(SetOperation.Intersect, "sist_dest", "sist_k1", "sist_k2");
        Assert.Equal(2, count);

        var members = await _db.SetMembersAsync("sist_dest");
        Assert.Equal(2, members.Length);
        Assert.Contains(members, v => v == "b");
        Assert.Contains(members, v => v == "c");
    }

    [Fact]
    public async Task SInterStore_empty_intersection_removes_destination()
    {
        // Pre-populate destination to verify it gets deleted on empty result
        await _db.SetAddAsync("sist_emp_dest", "pre_existing");
        await _db.SetAddAsync("sist_emp_k1", new RedisValue[] { "a", "b" });
        await _db.SetAddAsync("sist_emp_k2", new RedisValue[] { "c", "d" });

        var count = await _db.SetCombineAndStoreAsync(SetOperation.Intersect, "sist_emp_dest", "sist_emp_k1", "sist_emp_k2");
        Assert.Equal(0, count);
        Assert.False(await _db.KeyExistsAsync("sist_emp_dest"));
    }

    // =====================================================================
    // SUNIONSTORE
    // Ref: https://redis.io/docs/latest/commands/sunionstore/
    //   "Store the union of multiple sets in a key."
    // =====================================================================

    [Fact]
    public async Task SUnionStore_stores_union()
    {
        await _db.SetAddAsync("sust_k1", new RedisValue[] { "a", "b", "c" });
        await _db.SetAddAsync("sust_k2", new RedisValue[] { "b", "c", "d", "e" });

        var count = await _db.SetCombineAndStoreAsync(SetOperation.Union, "sust_dest", "sust_k1", "sust_k2");
        Assert.Equal(5, count);

        var members = await _db.SetMembersAsync("sust_dest");
        Assert.Equal(5, members.Length);
        Assert.Contains(members, v => v == "a");
        Assert.Contains(members, v => v == "b");
        Assert.Contains(members, v => v == "c");
        Assert.Contains(members, v => v == "d");
        Assert.Contains(members, v => v == "e");
    }

    [Fact]
    public async Task SUnionStore_overwrites_existing_destination()
    {
        await _db.SetAddAsync("sust_ow_dest", new RedisValue[] { "old1", "old2" });
        await _db.SetAddAsync("sust_ow_k1", new RedisValue[] { "new1" });
        await _db.SetAddAsync("sust_ow_k2", new RedisValue[] { "new2" });

        var count = await _db.SetCombineAndStoreAsync(SetOperation.Union, "sust_ow_dest", "sust_ow_k1", "sust_ow_k2");
        Assert.Equal(2, count);

        // Old members should be gone
        Assert.False(await _db.SetContainsAsync("sust_ow_dest", "old1"));
        Assert.False(await _db.SetContainsAsync("sust_ow_dest", "old2"));
    }

    // =====================================================================
    // SDIFFSTORE
    // Ref: https://redis.io/docs/latest/commands/sdiffstore/
    //   "Store the difference of multiple sets in a key."
    // =====================================================================

    [Fact]
    public async Task SDiffStore_stores_difference()
    {
        await _db.SetAddAsync("sdst_k1", new RedisValue[] { "a", "b", "c", "d" });
        await _db.SetAddAsync("sdst_k2", new RedisValue[] { "b", "d" });

        var count = await _db.SetCombineAndStoreAsync(SetOperation.Difference, "sdst_dest", "sdst_k1", "sdst_k2");
        Assert.Equal(2, count);

        var members = await _db.SetMembersAsync("sdst_dest");
        Assert.Equal(2, members.Length);
        Assert.Contains(members, v => v == "a");
        Assert.Contains(members, v => v == "c");
    }

    [Fact]
    public async Task SDiffStore_empty_diff_removes_destination()
    {
        await _db.SetAddAsync("sdst_emp_dest", "pre_existing");
        await _db.SetAddAsync("sdst_emp_k1", new RedisValue[] { "a", "b" });
        await _db.SetAddAsync("sdst_emp_k2", new RedisValue[] { "a", "b", "c" });

        var count = await _db.SetCombineAndStoreAsync(SetOperation.Difference, "sdst_emp_dest", "sdst_emp_k1", "sdst_emp_k2");
        Assert.Equal(0, count);
        Assert.False(await _db.KeyExistsAsync("sdst_emp_dest"));
    }
}
