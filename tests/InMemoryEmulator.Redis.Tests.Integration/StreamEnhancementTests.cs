using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class StreamEnhancementTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public StreamEnhancementTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ========================================================================
    // XSETID tests
    // Ref: https://redis.io/docs/latest/commands/xsetid/
    // ========================================================================

    [Fact]
    public async Task XSetId_sets_last_id_of_stream()
    {
        // Arrange: create a stream with one entry
        var entryId = await _db.StreamAddAsync("xsetid_stream", new NameValueEntry[] { new("f", "v") });
        // Parse the auto-generated entry ID to use a value well above it
        var entryTs = long.Parse(entryId.ToString().Split('-')[0]);
        var highId = $"{entryTs + 1000000}-0";

        // Act: set the last ID to a value above the existing entry
        var result = await _db.ExecuteAsync("XSETID", "xsetid_stream", highId);

        // Assert: returns OK
        Assert.Equal("OK", result.ToString());

        // Adding with * should generate an ID after the XSETID value
        var newId = await _db.StreamAddAsync("xsetid_stream", new NameValueEntry[] { new("f2", "v2") });
        var parts = newId.ToString().Split('-');
        var ts = long.Parse(parts[0]);
        Assert.True(ts >= entryTs + 1000000,
            $"Expected timestamp >= {entryTs + 1000000}, got {ts}");
    }

    [Fact]
    public async Task XSetId_does_not_decrease_last_id()
    {
        // Arrange: create a stream and set a high ID
        var entryId = await _db.StreamAddAsync("xsetid_nodec", new NameValueEntry[] { new("f", "v") });
        var entryTs = long.Parse(entryId.ToString().Split('-')[0]);
        var highId = $"{entryTs + 2000000}-0";
        var midId = $"{entryTs + 1000000}-0";
        await _db.ExecuteAsync("XSETID", "xsetid_nodec", highId);

        // Act: try to set to a lower ID (but still above the entry)
        var result = await _db.ExecuteAsync("XSETID", "xsetid_nodec", midId);
        Assert.Equal("OK", result.ToString());

        // Assert: the auto-gen ID should still be based on the higher value
        var newId = await _db.StreamAddAsync("xsetid_nodec", new NameValueEntry[] { new("f2", "v2") });
        var parts = newId.ToString().Split('-');
        var ts = long.Parse(parts[0]);
        Assert.True(ts >= entryTs + 2000000,
            $"Expected timestamp >= {entryTs + 2000000} after XSETID with lower value, got {ts}");
    }

    [Fact]
    public async Task XSetId_on_nonexistent_key_returns_error()
    {
        // Ref: https://redis.io/docs/latest/commands/xsetid/
        //   Returns "ERR no such key" when the stream does not exist.
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.ExecuteAsync("XSETID", "nonexistent_stream", "1-0"));
        Assert.Contains("no such key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ========================================================================
    // XPENDING extended form tests
    // Ref: https://redis.io/docs/latest/commands/xpending/
    // ========================================================================

    [Fact]
    public async Task XPending_extended_returns_pending_entries()
    {
        // Arrange: add entries, create group, read via group to create pending entries
        var key = "xpending_ext";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("a", "1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("b", "2") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("c", "3") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");

        // Read all entries as consumer "alice"
        await _db.ExecuteAsync("XREADGROUP", "GROUP", "grp", "alice", "COUNT", "10", "STREAMS", key, ">");

        // Act: query extended XPENDING
        var result = await _db.ExecuteAsync("XPENDING", key, "grp", "-", "+", "10");

        // Assert: should return 3 pending entries
        Assert.Equal(3, result.Length);

        // Each entry should be [id, consumer, idle-ms, delivery-count]
        for (int i = 0; i < 3; i++)
        {
            var entry = result[i];
            Assert.Equal(4, entry.Length);
            // Consumer should be "alice"
            Assert.Equal("alice", entry[1].ToString());
            // Delivery count should be 1
            Assert.Equal(1, (long)entry[3]);
        }
    }

    [Fact]
    public async Task XPending_extended_filters_by_consumer()
    {
        // Arrange
        var key = "xpending_filter";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("a", "1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("b", "2") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");

        // alice reads first entry
        await _db.ExecuteAsync("XREADGROUP", "GROUP", "grp", "alice", "COUNT", "1", "STREAMS", key, ">");
        // bob reads second entry
        await _db.ExecuteAsync("XREADGROUP", "GROUP", "grp", "bob", "COUNT", "1", "STREAMS", key, ">");

        // Act: query only alice's pending entries
        var result = await _db.ExecuteAsync("XPENDING", key, "grp", "-", "+", "10", "alice");

        // Assert: should return exactly 1 entry for alice
        Assert.Equal(1, result.Length);
        Assert.Equal("alice", result[0][1].ToString());
    }

    [Fact]
    public async Task XPending_extended_with_count_limits_results()
    {
        // Arrange
        var key = "xpending_count";
        for (int i = 0; i < 5; i++)
            await _db.StreamAddAsync(key, new NameValueEntry[] { new("f", i.ToString()) });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");
        await _db.ExecuteAsync("XREADGROUP", "GROUP", "grp", "consumer1", "COUNT", "5", "STREAMS", key, ">");

        // Act: request only 2
        var result = await _db.ExecuteAsync("XPENDING", key, "grp", "-", "+", "2");

        // Assert: should return exactly 2
        Assert.Equal(2, result.Length);
    }

    // ========================================================================
    // XGROUP CREATECONSUMER tests
    // Ref: https://redis.io/docs/latest/commands/xgroup-createconsumer/
    // ========================================================================

    [Fact]
    public async Task XGroup_CreateConsumer_creates_new_consumer()
    {
        // Arrange
        var key = "xg_createcon";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f", "v") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");

        // Act: create consumer explicitly
        var result = await _db.ExecuteAsync("XGROUP", "CREATECONSUMER", key, "grp", "newconsumer");

        // Assert: returns 1 when created
        Assert.Equal(1, (long)result);

        // Verify consumer exists via XINFO CONSUMERS
        var info = await _db.ExecuteAsync("XINFO", "CONSUMERS", key, "grp");
        Assert.Equal(1, info.Length);
        Assert.Equal("newconsumer", info[0][1].ToString()); // name field value
    }

    [Fact]
    public async Task XGroup_CreateConsumer_returns_0_if_already_exists()
    {
        // Arrange
        var key = "xg_createcon_dup";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f", "v") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");

        // Create consumer first time
        await _db.ExecuteAsync("XGROUP", "CREATECONSUMER", key, "grp", "myconsumer");

        // Act: create same consumer again
        var result = await _db.ExecuteAsync("XGROUP", "CREATECONSUMER", key, "grp", "myconsumer");

        // Assert: returns 0 when already exists
        Assert.Equal(0, (long)result);
    }

    [Fact]
    public async Task XGroup_CreateConsumer_nonexistent_group_returns_error()
    {
        // Arrange
        var key = "xg_createcon_nogrp";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f", "v") });

        // Act & Assert: no group exists
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.ExecuteAsync("XGROUP", "CREATECONSUMER", key, "nogroup", "consumer"));
        Assert.Contains("NOGROUP", ex.Message);
    }

    // ========================================================================
    // XGROUP DELCONSUMER tests
    // Ref: https://redis.io/docs/latest/commands/xgroup-delconsumer/
    // ========================================================================

    [Fact]
    public async Task XGroup_DelConsumer_returns_pending_count_and_removes_consumer()
    {
        // Arrange: create stream, group, consume entries
        var key = "xg_delcon";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("a", "1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("b", "2") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");
        // consumer reads 2 entries (creating 2 pending)
        await _db.ExecuteAsync("XREADGROUP", "GROUP", "grp", "victim", "COUNT", "10", "STREAMS", key, ">");

        // Act: delete consumer
        var result = await _db.ExecuteAsync("XGROUP", "DELCONSUMER", key, "grp", "victim");

        // Assert: returns the number of pending entries the consumer had (2)
        Assert.Equal(2, (long)result);

        // Verify consumer no longer exists
        var info = await _db.ExecuteAsync("XINFO", "CONSUMERS", key, "grp");
        Assert.Equal(0, info.Length);
    }

    [Fact]
    public async Task XGroup_DelConsumer_returns_0_when_consumer_has_no_pending()
    {
        // Arrange
        var key = "xg_delcon_nopend";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f", "v") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");
        // Explicitly create consumer (no pending entries)
        await _db.ExecuteAsync("XGROUP", "CREATECONSUMER", key, "grp", "emptyconsumer");

        // Act
        var result = await _db.ExecuteAsync("XGROUP", "DELCONSUMER", key, "grp", "emptyconsumer");

        // Assert: 0 pending entries
        Assert.Equal(0, (long)result);
    }

    [Fact]
    public async Task XGroup_DelConsumer_nonexistent_group_returns_error()
    {
        var key = "xg_delcon_nogrp";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f", "v") });

        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.ExecuteAsync("XGROUP", "DELCONSUMER", key, "nogroup", "consumer"));
        Assert.Contains("NOGROUP", ex.Message);
    }

    // ========================================================================
    // XINFO CONSUMERS tests
    // Ref: https://redis.io/docs/latest/commands/xinfo-consumers/
    // ========================================================================

    [Fact]
    public async Task XInfo_Consumers_returns_consumer_info()
    {
        // Arrange
        var key = "xinfo_cons";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("a", "1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("b", "2") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");

        // Two consumers read entries
        await _db.ExecuteAsync("XREADGROUP", "GROUP", "grp", "alice", "COUNT", "1", "STREAMS", key, ">");
        await _db.ExecuteAsync("XREADGROUP", "GROUP", "grp", "bob", "COUNT", "1", "STREAMS", key, ">");

        // Act
        var result = await _db.ExecuteAsync("XINFO", "CONSUMERS", key, "grp");

        // Assert: 2 consumers
        Assert.Equal(2, result.Length);

        // Each consumer entry is flat key-value pairs: name, <name>, pending, <n>, idle, <ms>, inactive, <ms>
        // Find alice and bob
        var names = new List<string>();
        var pendings = new List<long>();
        for (int i = 0; i < result.Length; i++)
        {
            var consumer = result[i];
            // name is at index 1, pending at index 3
            names.Add(consumer[1].ToString());
            pendings.Add((long)consumer[3]);
        }

        Assert.Contains("alice", names);
        Assert.Contains("bob", names);
        // Each has 1 pending
        Assert.All(pendings, p => Assert.Equal(1, p));
    }

    [Fact]
    public async Task XInfo_Consumers_returns_empty_for_group_with_no_consumers()
    {
        // Arrange
        var key = "xinfo_cons_empty";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f", "v") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");

        // Act
        var result = await _db.ExecuteAsync("XINFO", "CONSUMERS", key, "grp");

        // Assert: empty array
        Assert.Equal(0, result.Length);
    }

    [Fact]
    public async Task XInfo_Consumers_nonexistent_group_returns_error()
    {
        var key = "xinfo_cons_nogrp";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f", "v") });

        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.ExecuteAsync("XINFO", "CONSUMERS", key, "nogroup"));
        Assert.Contains("NOGROUP", ex.Message);
    }

    [Fact]
    public async Task XInfo_Consumers_nonexistent_key_returns_error()
    {
        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.ExecuteAsync("XINFO", "CONSUMERS", "nosuchkey", "nogroup"));
        Assert.Contains("no such key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
