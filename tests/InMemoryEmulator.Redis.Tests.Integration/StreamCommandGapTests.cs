using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class StreamCommandGapTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public StreamCommandGapTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ═══════════════════════════════════════════════════════════════
    // XREAD tests
    // Ref: https://redis.io/docs/latest/commands/xread/
    //   "Read data from one or multiple streams, only returning entries
    //    with an ID greater than the last received ID reported by the caller."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task XRead_returns_entries_from_beginning()
    {
        // Arrange
        var key = "xread_basic";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f1", "v1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f2", "v2") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f3", "v3") });

        // Act: read from beginning (0-0)
        var result = await _db.StreamReadAsync(key, "0-0");

        // Assert
        Assert.NotNull(result);
        Assert.Equal(3, result.Length);
        Assert.Equal("v1", result[0].Values.First(v => v.Name == "f1").Value.ToString());
        Assert.Equal("v2", result[1].Values.First(v => v.Name == "f2").Value.ToString());
        Assert.Equal("v3", result[2].Values.First(v => v.Name == "f3").Value.ToString());
    }

    [Fact]
    public async Task XRead_with_count_limits_entries()
    {
        // Ref: https://redis.io/docs/latest/commands/xread/
        //   "The COUNT option ... limits the number of entries returned per stream."

        // Arrange
        var key = "xread_count";
        for (int i = 0; i < 5; i++)
            await _db.StreamAddAsync(key, new NameValueEntry[] { new("idx", i.ToString()) });

        // Act: read with count = 2
        var result = await _db.StreamReadAsync(key, "0-0", count: 2);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(2, result.Length);
        Assert.Equal("0", result[0].Values.First(v => v.Name == "idx").Value.ToString());
        Assert.Equal("1", result[1].Values.First(v => v.Name == "idx").Value.ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // XREVRANGE tests
    // Ref: https://redis.io/docs/latest/commands/xrevrange/
    //   "This command is exactly like XRANGE, but with the notable difference
    //    of returning the entries in reverse order."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task XRevRange_returns_entries_in_reverse_order()
    {
        // Arrange
        var key = "xrevrange_basic";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("a", "1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("b", "2") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("c", "3") });

        // Act: reverse range from + to -
        var result = await _db.StreamRangeAsync(key, "-", "+", count: 10, messageOrder: Order.Descending);

        // Assert: entries should be in reverse order (last added first)
        Assert.NotNull(result);
        Assert.Equal(3, result.Length);
        Assert.Equal("3", result[0].Values.First(v => v.Name == "c").Value.ToString());
        Assert.Equal("2", result[1].Values.First(v => v.Name == "b").Value.ToString());
        Assert.Equal("1", result[2].Values.First(v => v.Name == "a").Value.ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // XTRIM tests
    // Ref: https://redis.io/docs/latest/commands/xtrim/
    //   "XTRIM trims the stream by evicting older entries."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task XTrim_maxlen_trims_stream_to_max_length()
    {
        // Ref: https://redis.io/docs/latest/commands/xtrim/
        //   "XTRIM key MAXLEN [=|~] threshold"

        // Arrange
        var key = "xtrim_maxlen";
        for (int i = 0; i < 10; i++)
            await _db.StreamAddAsync(key, new NameValueEntry[] { new("idx", i.ToString()) });

        Assert.Equal(10, await _db.StreamLengthAsync(key));

        // Act: trim to 5
        var trimmed = await _db.StreamTrimAsync(key, 5);

        // Assert: 5 entries removed, 5 remain
        Assert.Equal(5, trimmed);
        Assert.Equal(5, await _db.StreamLengthAsync(key));

        // Remaining entries should be the last 5 (indices 5-9)
        var entries = await _db.StreamRangeAsync(key, "-", "+");
        Assert.Equal(5, entries.Length);
        Assert.Equal("5", entries[0].Values.First(v => v.Name == "idx").Value.ToString());
        Assert.Equal("9", entries[4].Values.First(v => v.Name == "idx").Value.ToString());
    }

    [Fact]
    public async Task XTrim_minid_trims_entries_older_than_id()
    {
        // Ref: https://redis.io/docs/latest/commands/xtrim/
        //   "XTRIM key MINID [=|~] threshold"

        // Arrange
        var key = "xtrim_minid";
        var ids = new List<RedisValue>();
        for (int i = 0; i < 5; i++)
            ids.Add(await _db.StreamAddAsync(key, new NameValueEntry[] { new("idx", i.ToString()) }));

        Assert.Equal(5, await _db.StreamLengthAsync(key));

        // Act: trim entries with ID less than the 3rd entry
        var result = await _db.ExecuteAsync("XTRIM", key, "MINID", ids[2].ToString());

        // Assert: first 2 entries should be removed
        Assert.Equal(2, (long)result);
        Assert.Equal(3, await _db.StreamLengthAsync(key));
    }

    // ═══════════════════════════════════════════════════════════════
    // XGROUP CREATE tests
    // Ref: https://redis.io/docs/latest/commands/xgroup-create/
    //   "Create a new consumer group uniquely identified by <groupname>
    //    for the stream stored at <key>."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task XGroup_Create_creates_consumer_group()
    {
        // Arrange
        var key = "xgrp_create";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f", "v") });

        // Act
        var created = await _db.StreamCreateConsumerGroupAsync(key, "mygroup", "$");

        // Assert
        Assert.True(created);
    }

    [Fact]
    public async Task XGroup_Create_with_dollar_reads_from_latest()
    {
        // Ref: https://redis.io/docs/latest/commands/xgroup-create/
        //   "$ ... the consumer group will only deliver new messages."

        // Arrange: add entries, then create group at $
        var key = "xgrp_dollar";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("old", "1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("old", "2") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp_dollar", "$");

        // Act: read as consumer — should get nothing since group starts at $
        var result = await _db.StreamReadGroupAsync(key, "grp_dollar", "consumer1", ">", count: 10);

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result);

        // Now add a new entry — consumer should see it
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("new", "3") });
        var result2 = await _db.StreamReadGroupAsync(key, "grp_dollar", "consumer1", ">", count: 10);
        Assert.Single(result2);
        Assert.Equal("3", result2[0].Values.First(v => v.Name == "new").Value.ToString());
    }

    [Fact]
    public async Task XGroup_Create_with_zero_reads_from_beginning()
    {
        // Ref: https://redis.io/docs/latest/commands/xgroup-create/
        //   "0 ... the consumer group will read from the beginning of the stream."

        // Arrange
        var key = "xgrp_zero";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("a", "1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("b", "2") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp_zero", "0");

        // Act: read as consumer — should see all existing entries
        var result = await _db.StreamReadGroupAsync(key, "grp_zero", "consumer1", ">", count: 10);

        // Assert
        Assert.Equal(2, result.Length);
    }

    // ═══════════════════════════════════════════════════════════════
    // XREADGROUP tests
    // Ref: https://redis.io/docs/latest/commands/xreadgroup/
    //   "The XREADGROUP command is a special version of the XREAD command
    //    with support for consumer groups."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task XReadGroup_reads_new_entries_for_consumer()
    {
        // Arrange
        var key = "xrg_basic";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f1", "v1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f2", "v2") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");

        // Act
        var result = await _db.StreamReadGroupAsync(key, "grp", "consumer1", ">", count: 10);

        // Assert
        Assert.Equal(2, result.Length);
    }

    // ═══════════════════════════════════════════════════════════════
    // XACK tests
    // Ref: https://redis.io/docs/latest/commands/xack/
    //   "The XACK command removes one or multiple messages from the
    //    Pending Entries List (PEL) of a stream consumer group."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task XAck_acknowledges_pending_message()
    {
        // Arrange
        var key = "xack_basic";
        var id = await _db.StreamAddAsync(key, new NameValueEntry[] { new("f", "v") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");
        await _db.StreamReadGroupAsync(key, "grp", "consumer1", ">", count: 10);

        // Act: acknowledge the message
        var acked = await _db.StreamAcknowledgeAsync(key, "grp", id);

        // Assert
        Assert.Equal(1, acked);
    }

    [Fact]
    public async Task XReadGroup_then_XAck_full_lifecycle()
    {
        // Ref: https://redis.io/docs/latest/commands/xreadgroup/
        // Ref: https://redis.io/docs/latest/commands/xack/
        //   Full consumer group lifecycle: create group, read, acknowledge, verify PEL is empty

        // Arrange
        var key = "xrg_lifecycle";
        var id1 = await _db.StreamAddAsync(key, new NameValueEntry[] { new("a", "1") });
        var id2 = await _db.StreamAddAsync(key, new NameValueEntry[] { new("b", "2") });
        var id3 = await _db.StreamAddAsync(key, new NameValueEntry[] { new("c", "3") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");

        // Act: read all as consumer
        var entries = await _db.StreamReadGroupAsync(key, "grp", "consumer1", ">", count: 10);
        Assert.Equal(3, entries.Length);

        // Check pending entries (summary form)
        var pending = await _db.StreamPendingAsync(key, "grp");
        Assert.Equal(3, pending.PendingMessageCount);

        // Acknowledge all
        var ack1 = await _db.StreamAcknowledgeAsync(key, "grp", id1);
        var ack2 = await _db.StreamAcknowledgeAsync(key, "grp", id2);
        var ack3 = await _db.StreamAcknowledgeAsync(key, "grp", id3);
        Assert.Equal(1, ack1);
        Assert.Equal(1, ack2);
        Assert.Equal(1, ack3);

        // After acknowledging all, pending count should be 0
        var pendingAfter = await _db.StreamPendingAsync(key, "grp");
        Assert.Equal(0, pendingAfter.PendingMessageCount);
    }

    // ═══════════════════════════════════════════════════════════════
    // XCLAIM tests
    // Ref: https://redis.io/docs/latest/commands/xclaim/
    //   "Changes (or acquires) ownership of a message in a consumer group."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task XClaim_claims_pending_message_from_another_consumer()
    {
        // Arrange: consumer1 reads entries, then consumer2 claims them
        var key = "xclaim_basic";
        var id1 = await _db.StreamAddAsync(key, new NameValueEntry[] { new("f1", "v1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("f2", "v2") });
        await _db.StreamCreateConsumerGroupAsync(key, "grp", "0");

        // consumer1 reads all
        await _db.StreamReadGroupAsync(key, "grp", "consumer1", ">", count: 10);

        // Act: consumer2 claims the first entry with minIdleTime=0 so it always qualifies
        var claimed = await _db.StreamClaimAsync(key, "grp", "consumer2", 0, new RedisValue[] { id1 });

        // Assert: should return the claimed entry
        Assert.Single(claimed);
        Assert.Equal(id1.ToString(), claimed[0].Id.ToString());
        Assert.Equal("v1", claimed[0].Values.First(v => v.Name == "f1").Value.ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // XINFO STREAM tests
    // Ref: https://redis.io/docs/latest/commands/xinfo-stream/
    //   "Returns information about the stream stored at key."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task XInfo_Stream_returns_stream_info()
    {
        // Arrange
        var key = "xinfo_stream";
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("a", "1") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("b", "2") });
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("c", "3") });

        // Act
        var result = await _db.ExecuteAsync("XINFO", "STREAM", key);

        // Assert: result is a flat array of key-value pairs
        // Should contain "length" with value 3
        Assert.False(result.IsNull);
        var arr = (RedisResult[])result!;

        // Find the "length" field
        bool foundLength = false;
        for (int i = 0; i < arr.Length - 1; i++)
        {
            if (arr[i].ToString() == "length")
            {
                Assert.Equal(3, (long)arr[i + 1]);
                foundLength = true;
                break;
            }
        }
        Assert.True(foundLength, "XINFO STREAM should return a 'length' field");
    }

    // ═══════════════════════════════════════════════════════════════
    // XADD with MAXLEN tests
    // Ref: https://redis.io/docs/latest/commands/xadd/
    //   "XADD key [MAXLEN [=|~] threshold] ... field value [field value ...]"
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task XAdd_with_maxlen_trims_stream_on_add()
    {
        // Arrange: add initial entries
        var key = "xadd_maxlen";
        for (int i = 0; i < 5; i++)
            await _db.StreamAddAsync(key, new NameValueEntry[] { new("idx", i.ToString()) });

        Assert.Equal(5, await _db.StreamLengthAsync(key));

        // Act: add with maxLength 3 — should trim oldest entries
        await _db.StreamAddAsync(key, new NameValueEntry[] { new("idx", "5") }, maxLength: 3);

        // Assert: stream should have at most 3 entries
        var len = await _db.StreamLengthAsync(key);
        Assert.True(len <= 3, $"Expected at most 3 entries after XADD with MAXLEN 3, got {len}");

        // Remaining entries should be the newest ones
        var entries = await _db.StreamRangeAsync(key, "-", "+");
        var lastIdx = entries[^1].Values.First(v => v.Name == "idx").Value.ToString();
        Assert.Equal("5", lastIdx);
    }

    // ═══════════════════════════════════════════════════════════════
    // XADD with NOMKSTREAM tests
    // Ref: https://redis.io/docs/latest/commands/xadd/
    //   "NOMKSTREAM — Don't create the stream if it doesn't exist."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task XAdd_with_nomkstream_does_not_create_stream_if_not_exists()
    {
        // Arrange: key does not exist
        var key = "xadd_nomkstream";

        // Act: XADD with NOMKSTREAM on non-existent key
        var result = await _db.ExecuteAsync("XADD", key, "NOMKSTREAM", "*", "f", "v");

        // Assert: should return nil (stream not created)
        Assert.True(result.IsNull, "XADD with NOMKSTREAM should return nil when stream does not exist");
        Assert.False(await _db.KeyExistsAsync(key));
    }
}
