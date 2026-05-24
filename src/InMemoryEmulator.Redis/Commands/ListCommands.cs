using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class ListCommands : ICommandHandler
{
    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "LPUSH" => LPush(context),
            "RPUSH" => RPush(context),
            "LPUSHX" => LPushX(context),
            "RPUSHX" => RPushX(context),
            "LPOP" => LPop(context),
            "RPOP" => RPop(context),
            "LLEN" => LLen(context),
            "LRANGE" => LRange(context),
            "LINDEX" => LIndex(context),
            "LSET" => LSet(context),
            "LINSERT" => LInsert(context),
            "LREM" => LRem(context),
            "LTRIM" => LTrim(context),
            "RPOPLPUSH" => RPopLPush(context),
            "LMOVE" => LMove(context),
            "LPOS" => LPos(context),
            "BLPOP" => BLPop(context),
            "BRPOP" => BRPop(context),
            "BLMOVE" => BLMove(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static RedisList GetOrCreateList(CommandContext ctx, string key)
    {
        var entry = ctx.Database.GetEntry(key);
        if (entry == null)
        {
            var list = new RedisList();
            ctx.Database.SetEntry(key, list);
            return list;
        }
        if (entry is not RedisList l) throw new WrongTypeException();
        return l;
    }

    private static ValueTask<RespValue> LPush(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var list = GetOrCreateList(ctx, key);
        for (int i = 1; i < ctx.Arguments.Length; i++)
            list.Items.AddFirst(ctx.GetArgBytes(i) ?? Array.Empty<byte>());
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(list.Items.Count));
    }

    private static ValueTask<RespValue> RPush(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var list = GetOrCreateList(ctx, key);
        for (int i = 1; i < ctx.Arguments.Length; i++)
            list.Items.AddLast(ctx.GetArgBytes(i) ?? Array.Empty<byte>());
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(list.Items.Count));
    }

    private static ValueTask<RespValue> LPushX(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.Zero);
        for (int i = 1; i < ctx.Arguments.Length; i++)
            entry.Items.AddFirst(ctx.GetArgBytes(i) ?? Array.Empty<byte>());
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(entry.Items.Count));
    }

    private static ValueTask<RespValue> RPushX(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.Zero);
        for (int i = 1; i < ctx.Arguments.Length; i++)
            entry.Items.AddLast(ctx.GetArgBytes(i) ?? Array.Empty<byte>());
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(entry.Items.Count));
    }

    private static ValueTask<RespValue> LPop(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null || entry.Items.Count == 0)
            return ValueTask.FromResult(RespValue.NullBulkString);

        int count = ctx.Arguments.Length > 1 ? (int)ctx.GetArgLong(1) : 0;
        if (count == 0)
        {
            var val = entry.Items.First!.Value;
            entry.Items.RemoveFirst();
            if (entry.Items.Count == 0) ctx.Database.RemoveEntry(key);
            else ctx.Database.IncrementVersion(key);
            return ValueTask.FromResult<RespValue>(new RespValue.BulkString(val));
        }

        var results = new List<RespValue>();
        for (int i = 0; i < count && entry.Items.Count > 0; i++)
        {
            results.Add(new RespValue.BulkString(entry.Items.First!.Value));
            entry.Items.RemoveFirst();
        }
        if (entry.Items.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> RPop(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null || entry.Items.Count == 0)
            return ValueTask.FromResult(RespValue.NullBulkString);

        int count = ctx.Arguments.Length > 1 ? (int)ctx.GetArgLong(1) : 0;
        if (count == 0)
        {
            var val = entry.Items.Last!.Value;
            entry.Items.RemoveLast();
            if (entry.Items.Count == 0) ctx.Database.RemoveEntry(key);
            else ctx.Database.IncrementVersion(key);
            return ValueTask.FromResult<RespValue>(new RespValue.BulkString(val));
        }

        var results = new List<RespValue>();
        for (int i = 0; i < count && entry.Items.Count > 0; i++)
        {
            results.Add(new RespValue.BulkString(entry.Items.Last!.Value));
            entry.Items.RemoveLast();
        }
        if (entry.Items.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> LLen(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisList>(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(entry?.Items.Count ?? 0));
    }

    private static ValueTask<RespValue> LRange(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var start = (int)ctx.GetArgLong(1);
        var stop = (int)ctx.GetArgLong(2);
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var len = entry.Items.Count;
        if (start < 0) start = Math.Max(len + start, 0);
        if (stop < 0) stop = len + stop;
        if (start > stop || start >= len) return ValueTask.FromResult(RespValue.EmptyArray);
        stop = Math.Min(stop, len - 1);

        var items = entry.Items.ToArray();
        var results = new RespValue[stop - start + 1];
        for (int i = start; i <= stop; i++)
            results[i - start] = new RespValue.BulkString(items[i]);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> LIndex(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var index = (int)ctx.GetArgLong(1);
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.NullBulkString);

        var len = entry.Items.Count;
        if (index < 0) index = len + index;
        if (index < 0 || index >= len) return ValueTask.FromResult(RespValue.NullBulkString);

        var items = entry.Items.ToArray();
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(items[index]));
    }

    private static ValueTask<RespValue> LSet(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var index = (int)ctx.GetArgLong(1);
        var value = ctx.GetArgBytes(2) ?? Array.Empty<byte>();
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "no such key"));

        var len = entry.Items.Count;
        if (index < 0) index = len + index;
        if (index < 0 || index >= len)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "index out of range"));

        var node = entry.Items.First!;
        for (int i = 0; i < index; i++) node = node.Next!;
        node.Value = value;
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> LInsert(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var position = ctx.GetArgString(1).ToUpperInvariant();
        var pivot = ctx.GetArgBytes(2);
        var value = ctx.GetArgBytes(3) ?? Array.Empty<byte>();
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.Zero);

        var node = entry.Items.First;
        while (node != null)
        {
            if (node.Value.AsSpan().SequenceEqual(pivot))
            {
                if (position == "BEFORE")
                    entry.Items.AddBefore(node, value);
                else
                    entry.Items.AddAfter(node, value);
                ctx.Database.IncrementVersion(key);
                return ValueTask.FromResult<RespValue>(new RespValue.Integer(entry.Items.Count));
            }
            node = node.Next;
        }
        return ValueTask.FromResult(RespValue.NegativeOne);
    }

    private static ValueTask<RespValue> LRem(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var count = (int)ctx.GetArgLong(1);
        var value = ctx.GetArgBytes(2);
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.Zero);

        int removed = 0;
        if (count == 0)
        {
            var node = entry.Items.First;
            while (node != null)
            {
                var next = node.Next;
                if (node.Value.AsSpan().SequenceEqual(value)) { entry.Items.Remove(node); removed++; }
                node = next;
            }
        }
        else if (count > 0)
        {
            var node = entry.Items.First;
            while (node != null && removed < count)
            {
                var next = node.Next;
                if (node.Value.AsSpan().SequenceEqual(value)) { entry.Items.Remove(node); removed++; }
                node = next;
            }
        }
        else
        {
            var node = entry.Items.Last;
            var absCount = -count;
            while (node != null && removed < absCount)
            {
                var prev = node.Previous;
                if (node.Value.AsSpan().SequenceEqual(value)) { entry.Items.Remove(node); removed++; }
                node = prev;
            }
        }

        if (entry.Items.Count == 0) ctx.Database.RemoveEntry(key);
        else if (removed > 0) ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(removed));
    }

    private static ValueTask<RespValue> LTrim(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var start = (int)ctx.GetArgLong(1);
        var stop = (int)ctx.GetArgLong(2);
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.Ok);

        var len = entry.Items.Count;
        if (start < 0) start = Math.Max(len + start, 0);
        if (stop < 0) stop = len + stop;

        var items = entry.Items.ToArray();
        entry.Items.Clear();
        for (int i = start; i <= Math.Min(stop, len - 1); i++)
            entry.Items.AddLast(items[i]);

        if (entry.Items.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> RPopLPush(CommandContext ctx)
    {
        var srcKey = ctx.GetArgString(0);
        var dstKey = ctx.GetArgString(1);
        var src = ctx.Database.GetTyped<RedisList>(srcKey);
        if (src == null || src.Items.Count == 0)
            return ValueTask.FromResult(RespValue.NullBulkString);

        var val = src.Items.Last!.Value;
        src.Items.RemoveLast();
        if (src.Items.Count == 0) ctx.Database.RemoveEntry(srcKey);
        else ctx.Database.IncrementVersion(srcKey);

        var dst = GetOrCreateList(ctx, dstKey);
        dst.Items.AddFirst(val);
        ctx.Database.IncrementVersion(dstKey);
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(val));
    }

    private static ValueTask<RespValue> LMove(CommandContext ctx)
    {
        var srcKey = ctx.GetArgString(0);
        var dstKey = ctx.GetArgString(1);
        var srcDir = ctx.GetArgString(2).ToUpperInvariant();
        var dstDir = ctx.GetArgString(3).ToUpperInvariant();

        var src = ctx.Database.GetTyped<RedisList>(srcKey);
        if (src == null || src.Items.Count == 0)
            return ValueTask.FromResult(RespValue.NullBulkString);

        byte[] val;
        if (srcDir == "LEFT") { val = src.Items.First!.Value; src.Items.RemoveFirst(); }
        else { val = src.Items.Last!.Value; src.Items.RemoveLast(); }

        if (src.Items.Count == 0) ctx.Database.RemoveEntry(srcKey);
        else ctx.Database.IncrementVersion(srcKey);

        var dst = GetOrCreateList(ctx, dstKey);
        if (dstDir == "LEFT") dst.Items.AddFirst(val);
        else dst.Items.AddLast(val);
        ctx.Database.IncrementVersion(dstKey);

        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(val));
    }

    private static ValueTask<RespValue> LPos(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var element = ctx.GetArgBytes(1);
        var entry = ctx.Database.GetTyped<RedisList>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.NullBulkString);

        int rank = 1, count = 1, maxlen = 0;
        for (int i = 2; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "RANK": i++; rank = (int)ctx.GetArgLong(i); break;
                case "COUNT": i++; count = (int)ctx.GetArgLong(i); break;
                case "MAXLEN": i++; maxlen = (int)ctx.GetArgLong(i); break;
            }
        }

        var items = entry.Items.ToArray();
        var positions = new List<long>();
        int matchesFound = 0;
        int startIdx = rank > 0 ? 0 : items.Length - 1;
        int step = rank > 0 ? 1 : -1;
        int absRank = Math.Abs(rank);
        int skipped = 0;

        for (int i = startIdx; i >= 0 && i < items.Length; i += step)
        {
            if (maxlen > 0 && Math.Abs(i - startIdx) >= maxlen) break;
            if (items[i].AsSpan().SequenceEqual(element))
            {
                skipped++;
                if (skipped >= absRank)
                {
                    positions.Add(i);
                    matchesFound++;
                    if (count > 0 && matchesFound >= count) break;
                }
            }
        }

        if (count == 0 || positions.Count > 1)
            return ValueTask.FromResult<RespValue>(new RespValue.Array(
                positions.Select(p => (RespValue)new RespValue.Integer(p)).ToArray()));

        if (positions.Count == 0)
            return ValueTask.FromResult(RespValue.NullBulkString);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(positions[0]));
    }

    // Ref: https://redis.io/docs/latest/commands/blpop/
    //   "BLPOP is a blocking list pop primitive."
    // In the emulator: check immediately, return nil on timeout (no actual blocking)
    private static ValueTask<RespValue> BLPop(CommandContext ctx)
    {
        // Last argument is timeout, preceding are keys
        for (int i = 0; i < ctx.Arguments.Length - 1; i++)
        {
            var key = ctx.GetArgString(i);
            var entry = ctx.Database.GetTyped<RedisList>(key);
            if (entry != null && entry.Items.Count > 0)
            {
                var val = entry.Items.First!.Value;
                entry.Items.RemoveFirst();
                if (entry.Items.Count == 0) ctx.Database.RemoveEntry(key);
                else ctx.Database.IncrementVersion(key);
                return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
                {
                    RespValue.FromBulkString(key),
                    new RespValue.BulkString(val)
                }));
            }
        }
        return ValueTask.FromResult(RespValue.NullArray);
    }

    private static ValueTask<RespValue> BRPop(CommandContext ctx)
    {
        for (int i = 0; i < ctx.Arguments.Length - 1; i++)
        {
            var key = ctx.GetArgString(i);
            var entry = ctx.Database.GetTyped<RedisList>(key);
            if (entry != null && entry.Items.Count > 0)
            {
                var val = entry.Items.Last!.Value;
                entry.Items.RemoveLast();
                if (entry.Items.Count == 0) ctx.Database.RemoveEntry(key);
                else ctx.Database.IncrementVersion(key);
                return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
                {
                    RespValue.FromBulkString(key),
                    new RespValue.BulkString(val)
                }));
            }
        }
        return ValueTask.FromResult(RespValue.NullArray);
    }

    private static ValueTask<RespValue> BLMove(CommandContext ctx)
    {
        var srcKey = ctx.GetArgString(0);
        var dstKey = ctx.GetArgString(1);
        var srcDir = ctx.GetArgString(2).ToUpperInvariant();
        var dstDir = ctx.GetArgString(3).ToUpperInvariant();
        // timeout is arg 4, ignored in emulator

        var src = ctx.Database.GetTyped<RedisList>(srcKey);
        if (src == null || src.Items.Count == 0)
            return ValueTask.FromResult(RespValue.NullBulkString);

        byte[] val;
        if (srcDir == "LEFT") { val = src.Items.First!.Value; src.Items.RemoveFirst(); }
        else { val = src.Items.Last!.Value; src.Items.RemoveLast(); }

        if (src.Items.Count == 0) ctx.Database.RemoveEntry(srcKey);
        else ctx.Database.IncrementVersion(srcKey);

        var dst = GetOrCreateList(ctx, dstKey);
        if (dstDir == "LEFT") dst.Items.AddFirst(val);
        else dst.Items.AddLast(val);
        ctx.Database.IncrementVersion(dstKey);

        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(val));
    }
}
