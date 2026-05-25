using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class StreamCommands : ICommandHandler
{
    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "XADD" => XAdd(context),
            "XLEN" => XLen(context),
            "XRANGE" => XRange(context),
            "XREVRANGE" => XRevRange(context),
            "XREAD" => XRead(context),
            "XTRIM" => XTrim(context),
            "XDEL" => XDel(context),
            "XINFO" => XInfo(context),
            "XGROUP" => XGroup(context),
            "XREADGROUP" => XReadGroup(context),
            "XACK" => XAck(context),
            "XPENDING" => XPending(context),
            "XCLAIM" => XClaim(context),
            "XAUTOCLAIM" => XAutoClaim(context),
            "XSETID" => XSetId(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static RedisStream GetOrCreate(CommandContext ctx, string key)
    {
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) { var s = new RedisStream(); ctx.Database.SetEntry(key, s); return s; }
        if (entry is not RedisStream stream) throw new WrongTypeException();
        return stream;
    }

    private static ValueTask<RespValue> XAdd(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/xadd/
        var key = ctx.GetArgString(0);
        var stream = GetOrCreate(ctx, key);

        int i = 1;
        int maxLen = -1;

        // Parse options
        while (i < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "MAXLEN")
            {
                i++;
                if (ctx.GetArgString(i) == "~") i++; // approximate flag, ignore
                maxLen = (int)long.Parse(ctx.GetArgString(i));
                i++;
            }
            else if (opt == "NOMKSTREAM") { i++; }
            else break;
        }

        var idStr = ctx.GetArgString(i);
        i++;

        string id;
        if (idStr == "*")
        {
            id = stream.GenerateId();
        }
        else
        {
            if (stream.Entries.Count > 0 && RedisStream.CompareIds(idStr, stream.Entries[^1].Id) <= 0)
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                    "The ID specified in XADD is equal or smaller than the target stream top item"));
            id = idStr;
            var parts = id.Split('-');
            stream.LastTimestamp = long.Parse(parts[0]);
            stream.LastSequence = parts.Length > 1 ? long.Parse(parts[1]) : 0;
        }

        var fields = new Dictionary<string, byte[]>();
        while (i + 1 < ctx.Arguments.Length)
        {
            var field = ctx.GetArgString(i);
            var value = ctx.GetArgBytes(i + 1) ?? Array.Empty<byte>();
            fields[field] = value;
            i += 2;
        }

        stream.Entries.Add(new StreamEntry { Id = id, Fields = fields });

        // Trim
        if (maxLen >= 0)
        {
            while (stream.Entries.Count > maxLen)
                stream.Entries.RemoveAt(0);
        }

        ctx.Database.IncrementVersion(key);
        ctx.Database.NotifyKeyChanged(key);
        return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(id));
    }

    private static ValueTask<RespValue> XLen(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var stream = ctx.Database.GetTyped<RedisStream>(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(stream?.Entries.Count ?? 0));
    }

    private static ValueTask<RespValue> XRange(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var start = ctx.GetArgString(1);
        var end = ctx.GetArgString(2);
        int count = ctx.Arguments.Length > 3 && ctx.GetArgString(3).Equals("COUNT", StringComparison.OrdinalIgnoreCase)
            ? (int)ctx.GetArgLong(4) : int.MaxValue;

        var stream = ctx.Database.GetTyped<RedisStream>(key);
        if (stream == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var results = stream.Entries
            .Where(e => (start == "-" || RedisStream.CompareIds(e.Id, start) >= 0) &&
                       (end == "+" || RedisStream.CompareIds(e.Id, end) <= 0))
            .Take(count)
            .Select(FormatStreamEntry)
            .ToArray();

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> XRevRange(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var end = ctx.GetArgString(1);
        var start = ctx.GetArgString(2);
        int count = ctx.Arguments.Length > 3 && ctx.GetArgString(3).Equals("COUNT", StringComparison.OrdinalIgnoreCase)
            ? (int)ctx.GetArgLong(4) : int.MaxValue;

        var stream = ctx.Database.GetTyped<RedisStream>(key);
        if (stream == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var results = stream.Entries
            .Where(e => (start == "-" || RedisStream.CompareIds(e.Id, start) >= 0) &&
                       (end == "+" || RedisStream.CompareIds(e.Id, end) <= 0))
            .Reverse()
            .Take(count)
            .Select(FormatStreamEntry)
            .ToArray();

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static async ValueTask<RespValue> XRead(CommandContext ctx)
    {
        int count = int.MaxValue;
        long blockMs = -1;
        int i = 0;
        while (i < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "COUNT") { i++; count = (int)ctx.GetArgLong(i); i++; }
            else if (opt == "BLOCK") { i++; blockMs = ctx.GetArgLong(i); i++; }
            else if (opt == "STREAMS") { i++; break; }
            else { i++; }
        }

        var numStreams = (ctx.Arguments.Length - i) / 2;
        var keys = new string[numStreams];
        var ids = new string[numStreams];
        for (int j = 0; j < numStreams; j++) keys[j] = ctx.GetArgString(i + j);
        for (int j = 0; j < numStreams; j++) ids[j] = ctx.GetArgString(i + numStreams + j);

        var results = TryReadStreams(ctx, keys, ids, count);
        if (results.Count > 0)
            return new RespValue.Array(results.ToArray());

        // Blocking
        if (blockMs >= 0)
        {
            var timeout = blockMs == 0 ? TimeSpan.FromSeconds(5) : TimeSpan.FromMilliseconds(blockMs);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.CancellationToken);
            cts.CancelAfter(timeout);
            try
            {
                await ctx.Database.WaitForKeyChangeAsync(keys, cts.Token);
                results = TryReadStreams(ctx, keys, ids, count);
                if (results.Count > 0)
                    return new RespValue.Array(results.ToArray());
            }
            catch (OperationCanceledException) { }
        }

        return RespValue.NullArray;
    }

    private static List<RespValue> TryReadStreams(CommandContext ctx, string[] keys, string[] ids, int count)
    {
        var results = new List<RespValue>();
        for (int j = 0; j < keys.Length; j++)
        {
            var stream = ctx.Database.GetTyped<RedisStream>(keys[j]);
            if (stream == null) continue;

            var startId = ids[j] == "$" ? $"{stream.LastTimestamp}-{stream.LastSequence}" : ids[j];
            var entries = stream.Entries
                .Where(e => RedisStream.CompareIds(e.Id, startId) > 0)
                .Take(count)
                .Select(FormatStreamEntry)
                .ToArray();

            if (entries.Length > 0)
            {
                results.Add(new RespValue.Array(new RespValue[]
                {
                    RespValue.FromBulkString(keys[j]),
                    new RespValue.Array(entries)
                }));
            }
        }
        return results;
    }

    private static ValueTask<RespValue> XTrim(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var stream = ctx.Database.GetTyped<RedisStream>(key);
        if (stream == null) return ValueTask.FromResult(RespValue.Zero);

        int i = 1;
        var strategy = ctx.GetArgString(i).ToUpperInvariant();
        i++;
        if (i < ctx.Arguments.Length && ctx.GetArgString(i) == "~") i++; // approximate
        var thresholdStr = ctx.GetArgString(i);

        int removed = 0;
        if (strategy == "MAXLEN")
        {
            var maxLen = (int)long.Parse(thresholdStr);
            while (stream.Entries.Count > maxLen) { stream.Entries.RemoveAt(0); removed++; }
        }
        else if (strategy == "MINID")
        {
            // Remove entries with ID less than the threshold
            while (stream.Entries.Count > 0 && RedisStream.CompareIds(stream.Entries[0].Id, thresholdStr) < 0)
            {
                stream.Entries.RemoveAt(0);
                removed++;
            }
        }

        if (removed > 0) ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(removed));
    }

    private static ValueTask<RespValue> XDel(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var stream = ctx.Database.GetTyped<RedisStream>(key);
        if (stream == null) return ValueTask.FromResult(RespValue.Zero);

        int removed = 0;
        for (int i = 1; i < ctx.Arguments.Length; i++)
        {
            var id = ctx.GetArgString(i);
            removed += stream.Entries.RemoveAll(e => e.Id == id);
        }

        if (removed > 0) ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(removed));
    }

    private static ValueTask<RespValue> XInfo(CommandContext ctx)
    {
        var sub = ctx.GetArgString(0).ToUpperInvariant();
        if (sub == "STREAM")
        {
            var key = ctx.GetArgString(1);
            var stream = ctx.Database.GetTyped<RedisStream>(key);
            if (stream == null)
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "no such key"));

            return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
            {
                RespValue.FromBulkString("length"), new RespValue.Integer(stream.Entries.Count),
                RespValue.FromBulkString("first-entry"), stream.Entries.Count > 0 ? FormatStreamEntry(stream.Entries[0]) : RespValue.NullArray,
                RespValue.FromBulkString("last-entry"), stream.Entries.Count > 0 ? FormatStreamEntry(stream.Entries[^1]) : RespValue.NullArray,
                RespValue.FromBulkString("groups"), new RespValue.Integer(stream.ConsumerGroups.Count),
            }));
        }
        if (sub == "GROUPS")
        {
            var key = ctx.GetArgString(1);
            var stream = ctx.Database.GetTyped<RedisStream>(key);
            if (stream == null)
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "no such key"));

            var groups = stream.ConsumerGroups.Values.Select(g => new RespValue.Array(new RespValue[]
            {
                RespValue.FromBulkString("name"), RespValue.FromBulkString(g.Name),
                RespValue.FromBulkString("consumers"), new RespValue.Integer(g.Consumers.Count),
                RespValue.FromBulkString("pending"), new RespValue.Integer(g.PendingEntries.Count),
                RespValue.FromBulkString("last-delivered-id"), RespValue.FromBulkString(g.LastDeliveredId),
            })).ToArray();
            return ValueTask.FromResult<RespValue>(new RespValue.Array(groups));
        }
        // Ref: https://redis.io/docs/latest/commands/xinfo-consumers/
        //   "Returns a list of consumers in the consumer group."
        //   Each consumer: name, pending, idle, inactive (as flat key-value pairs)
        if (sub == "CONSUMERS")
        {
            var key = ctx.GetArgString(1);
            var groupName = ctx.GetArgString(2);
            var stream = ctx.Database.GetTyped<RedisStream>(key);
            if (stream == null)
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "no such key"));
            if (!stream.ConsumerGroups.TryGetValue(groupName, out var group))
                return ValueTask.FromResult<RespValue>(new RespValue.Error("NOGROUP",
                    $"No such consumer group '{groupName}' for key name '{key}'"));

            var now = DateTimeOffset.UtcNow;
            var consumers = group.Consumers.Values.Select(c =>
            {
                var idleMs = (long)(now - c.LastSeen).TotalMilliseconds;
                // Count actual pending entries for this consumer from the PEL
                var pending = group.PendingEntries.Values.Count(pe => pe.ConsumerName == c.Name);
                return new RespValue.Array(new RespValue[]
                {
                    RespValue.FromBulkString("name"), RespValue.FromBulkString(c.Name),
                    RespValue.FromBulkString("pending"), new RespValue.Integer(pending),
                    RespValue.FromBulkString("idle"), new RespValue.Integer(idleMs),
                    RespValue.FromBulkString("inactive"), new RespValue.Integer(idleMs),
                });
            }).ToArray();
            return ValueTask.FromResult<RespValue>(new RespValue.Array(consumers));
        }
        return ValueTask.FromResult(RespValue.EmptyArray);
    }

    private static ValueTask<RespValue> XGroup(CommandContext ctx)
    {
        var sub = ctx.GetArgString(0).ToUpperInvariant();
        if (sub == "CREATE")
        {
            var key = ctx.GetArgString(1);
            var groupName = ctx.GetArgString(2);
            var id = ctx.GetArgString(3);
            bool mkStream = ctx.Arguments.Length > 4 && ctx.GetArgString(4).Equals("MKSTREAM", StringComparison.OrdinalIgnoreCase);

            var stream = ctx.Database.GetTyped<RedisStream>(key);
            if (stream == null)
            {
                if (mkStream)
                {
                    stream = new RedisStream();
                    ctx.Database.SetEntry(key, stream);
                }
                else
                    return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "The XGROUP subcommand requires the key to exist."));
            }

            stream.ConsumerGroups[groupName] = new ConsumerGroup
            {
                Name = groupName,
                LastDeliveredId = id == "$" ? (stream.Entries.Count > 0 ? stream.Entries[^1].Id : "0-0") : id
            };
            return ValueTask.FromResult(RespValue.Ok);
        }
        if (sub == "DESTROY")
        {
            var key = ctx.GetArgString(1);
            var groupName = ctx.GetArgString(2);
            var stream = ctx.Database.GetTyped<RedisStream>(key);
            if (stream == null) return ValueTask.FromResult(RespValue.Zero);
            return ValueTask.FromResult<RespValue>(stream.ConsumerGroups.Remove(groupName) ? RespValue.One : RespValue.Zero);
        }
        if (sub == "SETID")
        {
            var key = ctx.GetArgString(1);
            var groupName = ctx.GetArgString(2);
            var id = ctx.GetArgString(3);
            var stream = ctx.Database.GetTyped<RedisStream>(key);
            if (stream == null || !stream.ConsumerGroups.TryGetValue(groupName, out var group))
                return ValueTask.FromResult<RespValue>(new RespValue.Error("NOGROUP", "No such consumer group"));
            group.LastDeliveredId = id;
            return ValueTask.FromResult(RespValue.Ok);
        }
        // Ref: https://redis.io/docs/latest/commands/xgroup-createconsumer/
        //   "Create a consumer named <consumername> in the consumer group <groupname>"
        //   "Returns 1 if the consumer was created, 0 if already existed"
        if (sub == "CREATECONSUMER")
        {
            var key = ctx.GetArgString(1);
            var groupName = ctx.GetArgString(2);
            var consumerName = ctx.GetArgString(3);
            var stream = ctx.Database.GetTyped<RedisStream>(key);
            if (stream == null || !stream.ConsumerGroups.TryGetValue(groupName, out var group))
                return ValueTask.FromResult<RespValue>(new RespValue.Error("NOGROUP",
                    $"No such consumer group '{groupName}' for key name '{key}'"));

            if (group.Consumers.ContainsKey(consumerName))
                return ValueTask.FromResult(RespValue.Zero);

            group.Consumers[consumerName] = new ConsumerInfo { Name = consumerName };
            return ValueTask.FromResult(RespValue.One);
        }
        // Ref: https://redis.io/docs/latest/commands/xgroup-delconsumer/
        //   "Delete a consumer named <consumername> in the consumer group <groupname>"
        //   "Returns the number of pending entries that the consumer had before it was deleted"
        if (sub == "DELCONSUMER")
        {
            var key = ctx.GetArgString(1);
            var groupName = ctx.GetArgString(2);
            var consumerName = ctx.GetArgString(3);
            var stream = ctx.Database.GetTyped<RedisStream>(key);
            if (stream == null || !stream.ConsumerGroups.TryGetValue(groupName, out var group))
                return ValueTask.FromResult<RespValue>(new RespValue.Error("NOGROUP",
                    $"No such consumer group '{groupName}' for key name '{key}'"));

            // Count pending entries for this consumer
            long pendingCount = group.PendingEntries.Values
                .Count(pe => pe.ConsumerName == consumerName);

            // Remove all pending entries for this consumer
            var idsToRemove = group.PendingEntries
                .Where(kv => kv.Value.ConsumerName == consumerName)
                .Select(kv => kv.Key)
                .ToList();
            foreach (var id in idsToRemove)
                group.PendingEntries.Remove(id);

            // Remove the consumer
            group.Consumers.Remove(consumerName);

            return ValueTask.FromResult<RespValue>(new RespValue.Integer(pendingCount));
        }
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> XReadGroup(CommandContext ctx)
    {
        string groupName = "", consumerName = "";
        int count = int.MaxValue;
        int i = 0;

        while (i < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "GROUP") { i++; groupName = ctx.GetArgString(i); i++; consumerName = ctx.GetArgString(i); i++; }
            else if (opt == "COUNT") { i++; count = (int)ctx.GetArgLong(i); i++; }
            else if (opt == "BLOCK") { i += 2; }
            else if (opt == "NOACK") { i++; }
            else if (opt == "STREAMS") { i++; break; }
            else { i++; }
        }

        var numStreams = (ctx.Arguments.Length - i) / 2;
        var keys = new string[numStreams];
        var ids = new string[numStreams];
        for (int j = 0; j < numStreams; j++) keys[j] = ctx.GetArgString(i + j);
        for (int j = 0; j < numStreams; j++) ids[j] = ctx.GetArgString(i + numStreams + j);

        var results = new List<RespValue>();
        for (int j = 0; j < numStreams; j++)
        {
            var stream = ctx.Database.GetTyped<RedisStream>(keys[j]);
            if (stream == null || !stream.ConsumerGroups.TryGetValue(groupName, out var group)) continue;

            if (!group.Consumers.ContainsKey(consumerName))
                group.Consumers[consumerName] = new ConsumerInfo { Name = consumerName };
            group.Consumers[consumerName].LastSeen = DateTimeOffset.UtcNow;

            IEnumerable<StreamEntry> entries;
            if (ids[j] == ">")
            {
                entries = stream.Entries
                    .Where(e => RedisStream.CompareIds(e.Id, group.LastDeliveredId) > 0)
                    .Take(count);
            }
            else
            {
                entries = group.PendingEntries.Values
                    .Where(pe => pe.ConsumerName == consumerName)
                    .Select(pe => stream.Entries.FirstOrDefault(e => e.Id == pe.EntryId))
                    .Where(e => e != null)
                    .Take(count)!;
            }

            var entryList = entries.ToList();
            if (ids[j] == ">")
            {
                foreach (var entry in entryList)
                {
                    group.LastDeliveredId = entry.Id;
                    group.PendingEntries[entry.Id] = new PendingEntry
                    {
                        EntryId = entry.Id,
                        ConsumerName = consumerName
                    };
                    group.Consumers[consumerName].PendingCount++;
                }
            }

            if (entryList.Count > 0)
            {
                results.Add(new RespValue.Array(new RespValue[]
                {
                    RespValue.FromBulkString(keys[j]),
                    new RespValue.Array(entryList.Select(FormatStreamEntry).ToArray())
                }));
            }
        }

        if (results.Count == 0) return ValueTask.FromResult(RespValue.NullArray);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> XAck(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var groupName = ctx.GetArgString(1);
        var stream = ctx.Database.GetTyped<RedisStream>(key);
        if (stream == null || !stream.ConsumerGroups.TryGetValue(groupName, out var group))
            return ValueTask.FromResult(RespValue.Zero);

        int acked = 0;
        for (int i = 2; i < ctx.Arguments.Length; i++)
        {
            var id = ctx.GetArgString(i);
            if (group.PendingEntries.Remove(id, out var pe))
            {
                if (group.Consumers.TryGetValue(pe.ConsumerName, out var consumer))
                    consumer.PendingCount--;
                acked++;
            }
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(acked));
    }

    private static ValueTask<RespValue> XPending(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var groupName = ctx.GetArgString(1);
        var stream = ctx.Database.GetTyped<RedisStream>(key);
        if (stream == null || !stream.ConsumerGroups.TryGetValue(groupName, out var group))
            return ValueTask.FromResult<RespValue>(new RespValue.Error("NOGROUP", "No such consumer group"));

        if (ctx.Arguments.Length == 2)
        {
            // Summary form
            var pending = group.PendingEntries;
            var minId = pending.Count > 0 ? pending.Keys.Min() : "";
            var maxId = pending.Count > 0 ? pending.Keys.Max() : "";
            var consumers = group.Consumers.Values
                .Where(c => c.PendingCount > 0)
                .Select(c => new RespValue.Array(new RespValue[]
                {
                    RespValue.FromBulkString(c.Name),
                    RespValue.FromBulkString(c.PendingCount.ToString())
                }))
                .ToArray();

            return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
            {
                new RespValue.Integer(pending.Count),
                pending.Count > 0 ? RespValue.FromBulkString(minId!) : RespValue.NullBulkString,
                pending.Count > 0 ? RespValue.FromBulkString(maxId!) : RespValue.NullBulkString,
                new RespValue.Array(consumers)
            }));
        }

        // Ref: https://redis.io/docs/latest/commands/xpending/
        //   Extended form: XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
        //   Returns array of [id, consumer-name, idle-time-ms, delivery-count] entries
        int argIdx = 2;
        long minIdleMs = 0;
        if (ctx.GetArgString(argIdx).Equals("IDLE", StringComparison.OrdinalIgnoreCase))
        {
            argIdx++;
            minIdleMs = long.Parse(ctx.GetArgString(argIdx));
            argIdx++;
        }

        var start = ctx.GetArgString(argIdx);
        argIdx++;
        var end = ctx.GetArgString(argIdx);
        argIdx++;
        var countArg = (int)long.Parse(ctx.GetArgString(argIdx));
        argIdx++;

        string? filterConsumer = null;
        if (argIdx < ctx.Arguments.Length)
        {
            filterConsumer = ctx.GetArgString(argIdx);
        }

        var now = DateTimeOffset.UtcNow;
        var filtered = group.PendingEntries.Values
            .Where(pe => start == "-" || RedisStream.CompareIds(pe.EntryId, start) >= 0)
            .Where(pe => end == "+" || RedisStream.CompareIds(pe.EntryId, end) <= 0)
            .Where(pe => filterConsumer == null || pe.ConsumerName == filterConsumer)
            .Where(pe => minIdleMs == 0 || (now - pe.DeliveredAt).TotalMilliseconds >= minIdleMs)
            .OrderBy(pe => pe.EntryId, StringComparer.Ordinal)
            .Take(countArg)
            .Select(pe => new RespValue.Array(new RespValue[]
            {
                RespValue.FromBulkString(pe.EntryId),
                RespValue.FromBulkString(pe.ConsumerName),
                new RespValue.Integer((long)(now - pe.DeliveredAt).TotalMilliseconds),
                new RespValue.Integer(pe.DeliveryCount)
            }))
            .ToArray();

        return ValueTask.FromResult<RespValue>(new RespValue.Array(filtered));
    }

    // Ref: https://redis.io/docs/latest/commands/xclaim/
    private static ValueTask<RespValue> XClaim(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var groupName = ctx.GetArgString(1);
        var consumerName = ctx.GetArgString(2);
        var minIdleTime = ctx.GetArgLong(3); // ignored in emulator

        var stream = ctx.Database.GetTyped<RedisStream>(key);
        if (stream == null || !stream.ConsumerGroups.TryGetValue(groupName, out var group))
            return ValueTask.FromResult(RespValue.EmptyArray);

        if (!group.Consumers.ContainsKey(consumerName))
            group.Consumers[consumerName] = new ConsumerInfo { Name = consumerName };

        var claimed = new List<RespValue>();
        for (int i = 4; i < ctx.Arguments.Length; i++)
        {
            var idStr = ctx.GetArgString(i);
            if (idStr.StartsWith('-') || idStr.Equals("IDLE", StringComparison.OrdinalIgnoreCase) ||
                idStr.Equals("TIME", StringComparison.OrdinalIgnoreCase) ||
                idStr.Equals("RETRYCOUNT", StringComparison.OrdinalIgnoreCase) ||
                idStr.Equals("FORCE", StringComparison.OrdinalIgnoreCase) ||
                idStr.Equals("JUSTID", StringComparison.OrdinalIgnoreCase))
                break;

            if (group.PendingEntries.TryGetValue(idStr, out var pe))
            {
                if (group.Consumers.TryGetValue(pe.ConsumerName, out var oldConsumer))
                    oldConsumer.PendingCount--;
                pe.ConsumerName = consumerName;
                pe.DeliveryCount++;
                pe.DeliveredAt = DateTimeOffset.UtcNow;
                group.Consumers[consumerName].PendingCount++;

                var entry = stream.Entries.FirstOrDefault(e => e.Id == idStr);
                if (entry != null)
                    claimed.Add(FormatStreamEntry(entry));
            }
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(claimed.ToArray()));
    }

    // Ref: https://redis.io/docs/latest/commands/xautoclaim/
    private static ValueTask<RespValue> XAutoClaim(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var groupName = ctx.GetArgString(1);
        var consumerName = ctx.GetArgString(2);
        var minIdleMs = ctx.GetArgLong(3);
        var startId = ctx.GetArgString(4);
        int count = int.MaxValue;
        bool justId = false;
        for (int i = 5; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "COUNT") { i++; count = (int)ctx.GetArgLong(i); }
            else if (opt == "JUSTID") justId = true;
        }

        var stream = ctx.Database.GetTyped<RedisStream>(key);
        if (stream == null || !stream.ConsumerGroups.TryGetValue(groupName, out var group))
            return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
            {
                RespValue.FromBulkString("0-0"),
                RespValue.EmptyArray,
                RespValue.EmptyArray
            }));

        if (!group.Consumers.ContainsKey(consumerName))
            group.Consumers[consumerName] = new ConsumerInfo { Name = consumerName };

        var minIdle = TimeSpan.FromMilliseconds(minIdleMs);
        var now = DateTimeOffset.UtcNow;
        var claimed = new List<RespValue>();
        var deletedIds = new List<RespValue>();
        string nextStartId = "0-0";

        var candidates = group.PendingEntries.Values
            .Where(pe => RedisStream.CompareIds(pe.EntryId, startId) >= 0)
            .Where(pe => (now - pe.DeliveredAt) >= minIdle)
            .OrderBy(pe => pe.EntryId)
            .Take(count)
            .ToList();

        foreach (var pe in candidates)
        {
            if (group.Consumers.TryGetValue(pe.ConsumerName, out var oldConsumer))
                oldConsumer.PendingCount--;
            pe.ConsumerName = consumerName;
            pe.DeliveryCount++;
            pe.DeliveredAt = now;
            group.Consumers[consumerName].PendingCount++;

            var entry = stream.Entries.FirstOrDefault(e => e.Id == pe.EntryId);
            if (entry != null)
            {
                claimed.Add(justId ? RespValue.FromBulkString(pe.EntryId) : FormatStreamEntry(entry));
            }
            else
            {
                deletedIds.Add(RespValue.FromBulkString(pe.EntryId));
                group.PendingEntries.Remove(pe.EntryId);
            }
            nextStartId = pe.EntryId;
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString(nextStartId),
            new RespValue.Array(claimed.ToArray()),
            new RespValue.Array(deletedIds.ToArray())
        }));
    }

    // Ref: https://redis.io/docs/latest/commands/xsetid/
    //   "XSETID is used to set the last ID of a stream without adding entries to it."
    //   "Normally the last ID of the stream is updated only when XADD is called."
    //   "Returns OK if the command was executed successfully."
    //   "The command only sets the last ID if it is greater than the current one."
    private static ValueTask<RespValue> XSetId(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var newId = ctx.GetArgString(1);

        var stream = ctx.Database.GetTyped<RedisStream>(key);
        if (stream == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "The XSETID subcommand requires the key to exist."));

        // Parse the new ID
        var parts = newId.Split('-');
        var newTs = long.Parse(parts[0]);
        var newSeq = parts.Length > 1 ? long.Parse(parts[1]) : 0L;

        // Only update if the new ID is greater than current
        var currentId = $"{stream.LastTimestamp}-{stream.LastSequence}";
        if (RedisStream.CompareIds(newId, currentId) > 0)
        {
            stream.LastTimestamp = newTs;
            stream.LastSequence = newSeq;
        }

        // Parse optional ENTRIESADDED — accepted but ignored in the emulator
        // (Redis uses it for internal bookkeeping)

        return ValueTask.FromResult(RespValue.Ok);
    }

    private static RespValue FormatStreamEntry(StreamEntry entry)
    {
        var fields = new List<RespValue>();
        foreach (var (field, value) in entry.Fields)
        {
            fields.Add(RespValue.FromBulkString(field));
            fields.Add(new RespValue.BulkString(value));
        }
        return new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString(entry.Id),
            new RespValue.Array(fields.ToArray())
        });
    }
}
