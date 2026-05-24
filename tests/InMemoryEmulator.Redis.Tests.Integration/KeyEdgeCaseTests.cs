using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class KeyEdgeCaseTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public KeyEdgeCaseTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Del_multiple_keys_returns_count()
    {
        await _db.StringSetAsync("del_a", "v");
        await _db.StringSetAsync("del_b", "v");
        var deleted = await _db.KeyDeleteAsync(new RedisKey[] { "del_a", "del_b", "del_c" });
        Assert.Equal(2, deleted);
    }

    [Fact]
    public async Task Del_nonexistent_returns_false()
    {
        var deleted = await _db.KeyDeleteAsync("del_ghost");
        Assert.False(deleted);
    }

    [Fact]
    public async Task Exists_with_multiple_counts_duplicates()
    {
        await _db.StringSetAsync("exists_multi", "v");
        // EXISTS key key key — counts 3 times in Redis
        var result = await _db.ExecuteAsync("EXISTS", "exists_multi", "exists_multi", "exists_multi");
        Assert.Equal(3, (long)result);
    }

    [Fact]
    public async Task Rename_nonexistent_source_throws()
    {
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.KeyRenameAsync("rename_ghost", "rename_dst"));
        Assert.Contains("no such key", ex.Message);
    }

    [Fact]
    public async Task Rename_overwrites_destination()
    {
        await _db.StringSetAsync("rename_src", "src_val");
        await _db.StringSetAsync("rename_dst", "dst_val");
        await _db.KeyRenameAsync("rename_src", "rename_dst");
        Assert.Equal("src_val", (await _db.StringGetAsync("rename_dst")).ToString());
        Assert.False(await _db.KeyExistsAsync("rename_src"));
    }

    [Fact]
    public async Task Type_returns_none_for_nonexistent()
    {
        var type = await _db.KeyTypeAsync("type_missing");
        Assert.Equal(RedisType.None, type);
    }

    [Fact]
    public async Task Type_returns_correct_for_all_types()
    {
        await _db.StringSetAsync("type_string", "v");
        await _db.ListRightPushAsync("type_list", "v");
        await _db.SetAddAsync("type_set", "v");
        await _db.SortedSetAddAsync("type_zset", "v", 1.0);
        await _db.HashSetAsync("type_hash", "f", "v");

        Assert.Equal(RedisType.String, await _db.KeyTypeAsync("type_string"));
        Assert.Equal(RedisType.List, await _db.KeyTypeAsync("type_list"));
        Assert.Equal(RedisType.Set, await _db.KeyTypeAsync("type_set"));
        Assert.Equal(RedisType.SortedSet, await _db.KeyTypeAsync("type_zset"));
        Assert.Equal(RedisType.Hash, await _db.KeyTypeAsync("type_hash"));
    }

    [Fact]
    public async Task Keys_pattern_matching()
    {
        await _db.StringSetAsync("user:1", "a");
        await _db.StringSetAsync("user:2", "b");
        await _db.StringSetAsync("order:1", "c");

        var mux = await _fixture.GetMultiplexerAsync();
        var server = mux.GetServers().First();
        var userKeys = server.Keys(pattern: "user:*").ToArray();
        Assert.Equal(2, userKeys.Length);
    }

    [Fact]
    public async Task FlushDb_removes_all_keys_in_current_db()
    {
        await _db.StringSetAsync("flush_a", "v");
        await _db.StringSetAsync("flush_b", "v");
        var mux = await _fixture.GetMultiplexerAsync();
        var server = mux.GetServers().First();
        await server.FlushDatabaseAsync();
        Assert.False(await _db.KeyExistsAsync("flush_a"));
        Assert.False(await _db.KeyExistsAsync("flush_b"));
    }

    [Fact]
    public async Task DbSize_returns_key_count()
    {
        await _db.StringSetAsync("dbsize_1", "v");
        await _db.StringSetAsync("dbsize_2", "v");
        await _db.StringSetAsync("dbsize_3", "v");
        var mux = await _fixture.GetMultiplexerAsync();
        var server = mux.GetServers().First();
        var size = await server.DatabaseSizeAsync();
        Assert.True(size >= 3);
    }

    [Fact]
    public async Task Copy_creates_independent_copy()
    {
        await _db.StringSetAsync("copy_src", "original");
        await _db.KeyCopyAsync("copy_src", "copy_dst");
        await _db.StringSetAsync("copy_src", "modified");
        Assert.Equal("original", (await _db.StringGetAsync("copy_dst")).ToString());
    }

    [Fact]
    public async Task Copy_to_existing_key_fails_without_replace()
    {
        await _db.StringSetAsync("copy_s", "val");
        await _db.StringSetAsync("copy_d", "existing");
        var result = await _db.KeyCopyAsync("copy_s", "copy_d");
        Assert.False(result);
    }
}
