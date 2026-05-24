using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class SetCommands : ICommandHandler
{
    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "SADD" => SAdd(context),
            "SREM" => SRem(context),
            "SMEMBERS" => SMembers(context),
            "SISMEMBER" => SIsMember(context),
            "SMISMEMBER" => SMisMember(context),
            "SCARD" => SCard(context),
            "SPOP" => SPop(context),
            "SRANDMEMBER" => SRandMember(context),
            "SINTER" => SInter(context),
            "SINTERCARD" => SInterCard(context),
            "SINTERSTORE" => SInterStore(context),
            "SUNION" => SUnion(context),
            "SUNIONSTORE" => SUnionStore(context),
            "SDIFF" => SDiff(context),
            "SDIFFSTORE" => SDiffStore(context),
            "SMOVE" => SMove(context),
            "SSCAN" => SScan(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static RedisSet GetOrCreate(CommandContext ctx, string key)
    {
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) { var s = new RedisSet(); ctx.Database.SetEntry(key, s); return s; }
        if (entry is not RedisSet set) throw new WrongTypeException();
        return set;
    }

    private static RedisSet? GetSet(CommandContext ctx, string key) => ctx.Database.GetTyped<RedisSet>(key);

    private static ValueTask<RespValue> SAdd(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var set = GetOrCreate(ctx, key);
        int added = 0;
        for (int i = 1; i < ctx.Arguments.Length; i++)
            if (set.Members.Add(ctx.GetArgString(i))) added++;
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(added));
    }

    private static ValueTask<RespValue> SRem(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var set = GetSet(ctx, key);
        if (set == null) return ValueTask.FromResult(RespValue.Zero);
        int removed = 0;
        for (int i = 1; i < ctx.Arguments.Length; i++)
            if (set.Members.Remove(ctx.GetArgString(i))) removed++;
        if (set.Members.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(removed));
    }

    private static ValueTask<RespValue> SMembers(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var set = GetSet(ctx, key);
        if (set == null) return ValueTask.FromResult(RespValue.EmptyArray);
        var results = set.Members.Select(m => (RespValue)RespValue.FromBulkString(m)).ToArray();
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> SIsMember(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var member = ctx.GetArgString(1);
        var set = GetSet(ctx, key);
        return ValueTask.FromResult<RespValue>(set != null && set.Members.Contains(member) ? RespValue.One : RespValue.Zero);
    }

    private static ValueTask<RespValue> SMisMember(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var set = GetSet(ctx, key);
        var results = new RespValue[ctx.Arguments.Length - 1];
        for (int i = 1; i < ctx.Arguments.Length; i++)
            results[i - 1] = set != null && set.Members.Contains(ctx.GetArgString(i)) ? RespValue.One : RespValue.Zero;
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> SCard(CommandContext ctx)
    {
        var set = GetSet(ctx, ctx.GetArgString(0));
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(set?.Members.Count ?? 0));
    }

    private static ValueTask<RespValue> SPop(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var set = GetSet(ctx, key);
        if (set == null || set.Members.Count == 0) return ValueTask.FromResult(RespValue.NullBulkString);

        int count = ctx.Arguments.Length > 1 ? (int)ctx.GetArgLong(1) : 0;
        if (count == 0)
        {
            var member = set.Members.ElementAt(Random.Shared.Next(set.Members.Count));
            set.Members.Remove(member);
            if (set.Members.Count == 0) ctx.Database.RemoveEntry(key);
            else ctx.Database.IncrementVersion(key);
            return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(member));
        }

        var results = new List<RespValue>();
        for (int i = 0; i < count && set.Members.Count > 0; i++)
        {
            var member = set.Members.ElementAt(Random.Shared.Next(set.Members.Count));
            set.Members.Remove(member);
            results.Add(RespValue.FromBulkString(member));
        }
        if (set.Members.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> SRandMember(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var set = GetSet(ctx, key);
        if (set == null || set.Members.Count == 0)
            return ValueTask.FromResult(ctx.Arguments.Length > 1 ? RespValue.EmptyArray : RespValue.NullBulkString);

        if (ctx.Arguments.Length == 1)
        {
            var member = set.Members.ElementAt(Random.Shared.Next(set.Members.Count));
            return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(member));
        }

        int count = (int)ctx.GetArgLong(1);
        var members = set.Members.ToArray();
        var results = new List<RespValue>();
        if (count >= 0)
        {
            var shuffled = members.OrderBy(_ => Random.Shared.Next()).Take(count);
            results.AddRange(shuffled.Select(m => (RespValue)RespValue.FromBulkString(m)));
        }
        else
        {
            for (int i = 0; i < -count; i++)
                results.Add(RespValue.FromBulkString(members[Random.Shared.Next(members.Length)]));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> SInter(CommandContext ctx)
    {
        var result = ComputeIntersection(ctx);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(
            result.Select(m => (RespValue)RespValue.FromBulkString(m)).ToArray()));
    }

    private static ValueTask<RespValue> SInterCard(CommandContext ctx)
    {
        var numKeys = (int)ctx.GetArgLong(0);
        var sets = new List<HashSet<string>>();
        for (int i = 1; i <= numKeys; i++)
        {
            var set = GetSet(ctx, ctx.GetArgString(i));
            sets.Add(set?.Members ?? new HashSet<string>());
        }
        int limit = 0;
        for (int i = numKeys + 1; i < ctx.Arguments.Length; i++)
        {
            if (ctx.GetArgString(i).Equals("LIMIT", StringComparison.OrdinalIgnoreCase))
            { i++; limit = (int)ctx.GetArgLong(i); }
        }
        if (sets.Count == 0) return ValueTask.FromResult(RespValue.Zero);
        var intersection = new HashSet<string>(sets[0]);
        for (int i = 1; i < sets.Count; i++) intersection.IntersectWith(sets[i]);
        var count = limit > 0 ? Math.Min(intersection.Count, limit) : intersection.Count;
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private static ValueTask<RespValue> SInterStore(CommandContext ctx)
    {
        var destKey = ctx.GetArgString(0);
        var result = ComputeIntersection(ctx, 1);
        if (result.Count == 0) { ctx.Database.RemoveEntry(destKey); return ValueTask.FromResult(RespValue.Zero); }
        var dest = new RedisSet();
        foreach (var m in result) dest.Members.Add(m);
        ctx.Database.SetEntry(destKey, dest);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(result.Count));
    }

    private static ValueTask<RespValue> SUnion(CommandContext ctx)
    {
        var result = ComputeUnion(ctx);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(
            result.Select(m => (RespValue)RespValue.FromBulkString(m)).ToArray()));
    }

    private static ValueTask<RespValue> SUnionStore(CommandContext ctx)
    {
        var destKey = ctx.GetArgString(0);
        var result = ComputeUnion(ctx, 1);
        if (result.Count == 0) { ctx.Database.RemoveEntry(destKey); return ValueTask.FromResult(RespValue.Zero); }
        var dest = new RedisSet();
        foreach (var m in result) dest.Members.Add(m);
        ctx.Database.SetEntry(destKey, dest);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(result.Count));
    }

    private static ValueTask<RespValue> SDiff(CommandContext ctx)
    {
        var result = ComputeDiff(ctx);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(
            result.Select(m => (RespValue)RespValue.FromBulkString(m)).ToArray()));
    }

    private static ValueTask<RespValue> SDiffStore(CommandContext ctx)
    {
        var destKey = ctx.GetArgString(0);
        var result = ComputeDiff(ctx, 1);
        if (result.Count == 0) { ctx.Database.RemoveEntry(destKey); return ValueTask.FromResult(RespValue.Zero); }
        var dest = new RedisSet();
        foreach (var m in result) dest.Members.Add(m);
        ctx.Database.SetEntry(destKey, dest);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(result.Count));
    }

    private static ValueTask<RespValue> SMove(CommandContext ctx)
    {
        var srcKey = ctx.GetArgString(0);
        var dstKey = ctx.GetArgString(1);
        var member = ctx.GetArgString(2);
        var src = GetSet(ctx, srcKey);
        if (src == null || !src.Members.Remove(member))
            return ValueTask.FromResult(RespValue.Zero);
        if (src.Members.Count == 0) ctx.Database.RemoveEntry(srcKey);
        else ctx.Database.IncrementVersion(srcKey);
        var dst = GetOrCreate(ctx, dstKey);
        dst.Members.Add(member);
        ctx.Database.IncrementVersion(dstKey);
        return ValueTask.FromResult(RespValue.One);
    }

    private static ValueTask<RespValue> SScan(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var cursor = ctx.GetArgLong(1);
        string? pattern = null; int count = 10;
        for (int i = 2; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "MATCH") { i++; pattern = ctx.GetArgString(i); }
            else if (opt == "COUNT") { i++; count = (int)ctx.GetArgLong(i); }
        }
        var set = GetSet(ctx, key);
        if (set == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[] { RespValue.FromBulkString("0"), RespValue.EmptyArray }));

        var members = set.Members
            .Where(m => pattern == null || ServerCommands.MatchesGlob(m, pattern))
            .OrderBy(m => m).ToList();
        var start = (int)cursor;
        var end = Math.Min(start + count, members.Count);
        var next = end >= members.Count ? 0 : end;
        var results = members.Skip(start).Take(count).Select(m => (RespValue)RespValue.FromBulkString(m)).ToArray();
        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[] { RespValue.FromBulkString(next.ToString()), new RespValue.Array(results) }));
    }

    private static HashSet<string> ComputeIntersection(CommandContext ctx, int startIdx = 0)
    {
        var sets = new List<HashSet<string>>();
        for (int i = startIdx; i < ctx.Arguments.Length; i++)
        {
            var set = GetSet(ctx, ctx.GetArgString(i));
            sets.Add(set?.Members ?? new HashSet<string>());
        }
        if (sets.Count == 0) return new HashSet<string>();
        var result = new HashSet<string>(sets[0]);
        for (int i = 1; i < sets.Count; i++) result.IntersectWith(sets[i]);
        return result;
    }

    private static HashSet<string> ComputeUnion(CommandContext ctx, int startIdx = 0)
    {
        var result = new HashSet<string>();
        for (int i = startIdx; i < ctx.Arguments.Length; i++)
        {
            var set = GetSet(ctx, ctx.GetArgString(i));
            if (set != null) result.UnionWith(set.Members);
        }
        return result;
    }

    private static HashSet<string> ComputeDiff(CommandContext ctx, int startIdx = 0)
    {
        var first = GetSet(ctx, ctx.GetArgString(startIdx));
        if (first == null) return new HashSet<string>();
        var result = new HashSet<string>(first.Members);
        for (int i = startIdx + 1; i < ctx.Arguments.Length; i++)
        {
            var set = GetSet(ctx, ctx.GetArgString(i));
            if (set != null) result.ExceptWith(set.Members);
        }
        return result;
    }
}
