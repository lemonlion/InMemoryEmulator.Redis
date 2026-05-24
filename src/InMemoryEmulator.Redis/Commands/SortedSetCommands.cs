using System.Globalization;
using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class SortedSetCommands : ICommandHandler
{
    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "ZADD" => ZAdd(context),
            "ZREM" => ZRem(context),
            "ZSCORE" => ZScore(context),
            "ZMSCORE" => ZMScore(context),
            "ZRANK" => ZRank(context),
            "ZREVRANK" => ZRevRank(context),
            "ZCARD" => ZCard(context),
            "ZCOUNT" => ZCount(context),
            "ZINCRBY" => ZIncrBy(context),
            "ZRANGE" => ZRange(context),
            "ZREVRANGE" => ZRevRange(context),
            "ZRANGEBYSCORE" => ZRangeByScore(context),
            "ZREVRANGEBYSCORE" => ZRevRangeByScore(context),
            "ZRANGEBYLEX" => ZRangeByLex(context),
            "ZREVRANGEBYLEX" => ZRevRangeByLex(context),
            "ZLEXCOUNT" => ZLexCount(context),
            "ZPOPMIN" => ZPopMin(context),
            "ZPOPMAX" => ZPopMax(context),
            "ZRANDMEMBER" => ZRandMember(context),
            "ZUNIONSTORE" => ZUnionStore(context),
            "ZINTERSTORE" => ZInterStore(context),
            "ZDIFFSTORE" => ZDiffStore(context),
            "ZSCAN" => ZScan(context),
            "ZRANGESTORE" => ZRangeStore(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static RedisSortedSet GetOrCreate(CommandContext ctx, string key)
    {
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) { var z = new RedisSortedSet(); ctx.Database.SetEntry(key, z); return z; }
        if (entry is not RedisSortedSet zs) throw new WrongTypeException();
        return zs;
    }

    private static RedisSortedSet? GetZSet(CommandContext ctx, string key) => ctx.Database.GetTyped<RedisSortedSet>(key);

    private static ValueTask<RespValue> ZAdd(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        bool nx = false, xx = false, gt = false, lt = false, ch = false;
        int i = 1;
        while (i < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "NX") { nx = true; i++; }
            else if (opt == "XX") { xx = true; i++; }
            else if (opt == "GT") { gt = true; i++; }
            else if (opt == "LT") { lt = true; i++; }
            else if (opt == "CH") { ch = true; i++; }
            else break;
        }

        var zset = GetOrCreate(ctx, key);
        int added = 0, changed = 0;
        while (i + 1 < ctx.Arguments.Length)
        {
            var score = ParseScore(ctx.GetArgString(i));
            var member = ctx.GetArgString(i + 1);
            i += 2;

            var existing = zset.GetScore(member);
            if (nx && existing.HasValue) continue;
            if (xx && !existing.HasValue) continue;
            if (existing.HasValue)
            {
                if (gt && score <= existing.Value) continue;
                if (lt && score >= existing.Value) continue;
                if (score != existing.Value) { zset.Add(member, score); changed++; }
            }
            else
            {
                zset.Add(member, score);
                added++;
            }
        }
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(ch ? added + changed : added));
    }

    private static ValueTask<RespValue> ZRem(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.Zero);
        int removed = 0;
        for (int i = 1; i < ctx.Arguments.Length; i++)
            if (zset.Remove(ctx.GetArgString(i))) removed++;
        if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(removed));
    }

    private static ValueTask<RespValue> ZScore(CommandContext ctx)
    {
        var zset = GetZSet(ctx, ctx.GetArgString(0));
        if (zset == null) return ValueTask.FromResult(RespValue.NullBulkString);
        var score = zset.GetScore(ctx.GetArgString(1));
        if (!score.HasValue) return ValueTask.FromResult(RespValue.NullBulkString);
        return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(FormatScore(score.Value)));
    }

    private static ValueTask<RespValue> ZMScore(CommandContext ctx)
    {
        var zset = GetZSet(ctx, ctx.GetArgString(0));
        var results = new RespValue[ctx.Arguments.Length - 1];
        for (int i = 1; i < ctx.Arguments.Length; i++)
        {
            var score = zset?.GetScore(ctx.GetArgString(i));
            results[i - 1] = score.HasValue ? RespValue.FromBulkString(FormatScore(score.Value)) : RespValue.NullBulkString;
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> ZRank(CommandContext ctx)
    {
        var zset = GetZSet(ctx, ctx.GetArgString(0));
        if (zset == null) return ValueTask.FromResult(RespValue.NullBulkString);
        var rank = zset.GetRank(ctx.GetArgString(1));
        if (rank == null) return ValueTask.FromResult(RespValue.NullBulkString);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(rank.Value));
    }

    private static ValueTask<RespValue> ZRevRank(CommandContext ctx)
    {
        var zset = GetZSet(ctx, ctx.GetArgString(0));
        if (zset == null) return ValueTask.FromResult(RespValue.NullBulkString);
        var rank = zset.GetRevRank(ctx.GetArgString(1));
        if (rank == null) return ValueTask.FromResult(RespValue.NullBulkString);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(rank.Value));
    }

    private static ValueTask<RespValue> ZCard(CommandContext ctx)
    {
        var zset = GetZSet(ctx, ctx.GetArgString(0));
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(zset?.MemberScores.Count ?? 0));
    }

    private static ValueTask<RespValue> ZCount(CommandContext ctx)
    {
        var zset = GetZSet(ctx, ctx.GetArgString(0));
        if (zset == null) return ValueTask.FromResult(RespValue.Zero);
        var (min, minExcl) = ParseScoreBound(ctx.GetArgString(1), double.NegativeInfinity);
        var (max, maxExcl) = ParseScoreBound(ctx.GetArgString(2), double.PositiveInfinity);
        var count = zset.ScoreIndex.Count(e =>
            (minExcl ? e.Score > min : e.Score >= min) &&
            (maxExcl ? e.Score < max : e.Score <= max));
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private static ValueTask<RespValue> ZIncrBy(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var increment = ParseScore(ctx.GetArgString(1));
        var member = ctx.GetArgString(2);
        var zset = GetOrCreate(ctx, key);
        var current = zset.GetScore(member) ?? 0;
        var newScore = current + increment;
        zset.Add(member, newScore);
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(FormatScore(newScore)));
    }

    private static ValueTask<RespValue> ZRange(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var start = (int)ctx.GetArgLong(1);
        var stop = (int)ctx.GetArgLong(2);
        bool withScores = ctx.Arguments.Length > 3 && ctx.GetArgString(3).Equals("WITHSCORES", StringComparison.OrdinalIgnoreCase);

        var items = zset.ScoreIndex.ToList();
        var len = items.Count;
        if (start < 0) start = Math.Max(len + start, 0);
        if (stop < 0) stop = len + stop;
        if (start > stop || start >= len) return ValueTask.FromResult(RespValue.EmptyArray);
        stop = Math.Min(stop, len - 1);

        var results = new List<RespValue>();
        for (int i = start; i <= stop; i++)
        {
            results.Add(RespValue.FromBulkString(items[i].Member));
            if (withScores) results.Add(RespValue.FromBulkString(FormatScore(items[i].Score)));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> ZRevRange(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var start = (int)ctx.GetArgLong(1);
        var stop = (int)ctx.GetArgLong(2);
        bool withScores = ctx.Arguments.Length > 3 && ctx.GetArgString(3).Equals("WITHSCORES", StringComparison.OrdinalIgnoreCase);

        var items = zset.ScoreIndex.Reverse().ToList();
        var len = items.Count;
        if (start < 0) start = Math.Max(len + start, 0);
        if (stop < 0) stop = len + stop;
        if (start > stop || start >= len) return ValueTask.FromResult(RespValue.EmptyArray);
        stop = Math.Min(stop, len - 1);

        var results = new List<RespValue>();
        for (int i = start; i <= stop; i++)
        {
            results.Add(RespValue.FromBulkString(items[i].Member));
            if (withScores) results.Add(RespValue.FromBulkString(FormatScore(items[i].Score)));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> ZRangeByScore(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var (min, minExcl) = ParseScoreBound(ctx.GetArgString(1), double.NegativeInfinity);
        var (max, maxExcl) = ParseScoreBound(ctx.GetArgString(2), double.PositiveInfinity);
        bool withScores = false; int offset = 0, count = int.MaxValue;
        for (int i = 3; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "WITHSCORES") withScores = true;
            else if (opt == "LIMIT") { i++; offset = (int)ctx.GetArgLong(i); i++; count = (int)ctx.GetArgLong(i); }
        }

        var filtered = zset.ScoreIndex
            .Where(e => (minExcl ? e.Score > min : e.Score >= min) && (maxExcl ? e.Score < max : e.Score <= max))
            .Skip(offset).Take(count);

        var results = new List<RespValue>();
        foreach (var (score, member) in filtered)
        {
            results.Add(RespValue.FromBulkString(member));
            if (withScores) results.Add(RespValue.FromBulkString(FormatScore(score)));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> ZRevRangeByScore(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var (max, maxExcl) = ParseScoreBound(ctx.GetArgString(1), double.PositiveInfinity);
        var (min, minExcl) = ParseScoreBound(ctx.GetArgString(2), double.NegativeInfinity);
        bool withScores = false; int offset = 0, count = int.MaxValue;
        for (int i = 3; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "WITHSCORES") withScores = true;
            else if (opt == "LIMIT") { i++; offset = (int)ctx.GetArgLong(i); i++; count = (int)ctx.GetArgLong(i); }
        }

        var filtered = zset.ScoreIndex.Reverse()
            .Where(e => (minExcl ? e.Score > min : e.Score >= min) && (maxExcl ? e.Score < max : e.Score <= max))
            .Skip(offset).Take(count);

        var results = new List<RespValue>();
        foreach (var (score, member) in filtered)
        {
            results.Add(RespValue.FromBulkString(member));
            if (withScores) results.Add(RespValue.FromBulkString(FormatScore(score)));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> ZRangeByLex(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var (min, minExcl, minInf) = ParseLexBound(ctx.GetArgString(1));
        var (max, maxExcl, maxInf) = ParseLexBound(ctx.GetArgString(2));
        int offset = 0, count = int.MaxValue;
        for (int i = 3; i < ctx.Arguments.Length; i++)
        {
            if (ctx.GetArgString(i).Equals("LIMIT", StringComparison.OrdinalIgnoreCase))
            { i++; offset = (int)ctx.GetArgLong(i); i++; count = (int)ctx.GetArgLong(i); }
        }

        var filtered = zset.ScoreIndex
            .Select(e => e.Member)
            .Where(m => (minInf || (minExcl ? string.CompareOrdinal(m, min) > 0 : string.CompareOrdinal(m, min) >= 0)) &&
                       (maxInf || (maxExcl ? string.CompareOrdinal(m, max) < 0 : string.CompareOrdinal(m, max) <= 0)))
            .Skip(offset).Take(count)
            .Select(m => (RespValue)RespValue.FromBulkString(m)).ToArray();

        return ValueTask.FromResult<RespValue>(new RespValue.Array(filtered));
    }

    private static ValueTask<RespValue> ZRevRangeByLex(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var (max, maxExcl, maxInf) = ParseLexBound(ctx.GetArgString(1));
        var (min, minExcl, minInf) = ParseLexBound(ctx.GetArgString(2));
        int offset = 0, count = int.MaxValue;
        for (int i = 3; i < ctx.Arguments.Length; i++)
        {
            if (ctx.GetArgString(i).Equals("LIMIT", StringComparison.OrdinalIgnoreCase))
            { i++; offset = (int)ctx.GetArgLong(i); i++; count = (int)ctx.GetArgLong(i); }
        }

        var filtered = zset.ScoreIndex.Reverse()
            .Select(e => e.Member)
            .Where(m => (minInf || (minExcl ? string.CompareOrdinal(m, min) > 0 : string.CompareOrdinal(m, min) >= 0)) &&
                       (maxInf || (maxExcl ? string.CompareOrdinal(m, max) < 0 : string.CompareOrdinal(m, max) <= 0)))
            .Skip(offset).Take(count)
            .Select(m => (RespValue)RespValue.FromBulkString(m)).ToArray();

        return ValueTask.FromResult<RespValue>(new RespValue.Array(filtered));
    }

    private static ValueTask<RespValue> ZLexCount(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.Zero);
        var (min, minExcl, minInf) = ParseLexBound(ctx.GetArgString(1));
        var (max, maxExcl, maxInf) = ParseLexBound(ctx.GetArgString(2));
        var count = zset.ScoreIndex
            .Select(e => e.Member)
            .Count(m => (minInf || (minExcl ? string.CompareOrdinal(m, min) > 0 : string.CompareOrdinal(m, min) >= 0)) &&
                       (maxInf || (maxExcl ? string.CompareOrdinal(m, max) < 0 : string.CompareOrdinal(m, max) <= 0)));
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private static ValueTask<RespValue> ZPopMin(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);
        int count = ctx.Arguments.Length > 1 ? (int)ctx.GetArgLong(1) : 1;
        var results = new List<RespValue>();
        for (int i = 0; i < count && zset.ScoreIndex.Count > 0; i++)
        {
            var item = zset.ScoreIndex.Min;
            zset.Remove(item.Member);
            results.Add(RespValue.FromBulkString(item.Member));
            results.Add(RespValue.FromBulkString(FormatScore(item.Score)));
        }
        if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> ZPopMax(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);
        int count = ctx.Arguments.Length > 1 ? (int)ctx.GetArgLong(1) : 1;
        var results = new List<RespValue>();
        for (int i = 0; i < count && zset.ScoreIndex.Count > 0; i++)
        {
            var item = zset.ScoreIndex.Max;
            zset.Remove(item.Member);
            results.Add(RespValue.FromBulkString(item.Member));
            results.Add(RespValue.FromBulkString(FormatScore(item.Score)));
        }
        if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> ZRandMember(CommandContext ctx)
    {
        var zset = GetZSet(ctx, ctx.GetArgString(0));
        if (zset == null || zset.MemberScores.Count == 0)
            return ValueTask.FromResult(ctx.Arguments.Length > 1 ? RespValue.EmptyArray : RespValue.NullBulkString);
        var members = zset.MemberScores.Keys.ToArray();
        if (ctx.Arguments.Length == 1)
            return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(members[Random.Shared.Next(members.Length)]));

        int count = (int)ctx.GetArgLong(1);
        bool withScores = ctx.Arguments.Length > 2 && ctx.GetArgString(2).Equals("WITHSCORES", StringComparison.OrdinalIgnoreCase);
        var results = new List<RespValue>();
        var absCount = Math.Abs(count);
        for (int i = 0; i < absCount; i++)
        {
            var m = members[Random.Shared.Next(members.Length)];
            results.Add(RespValue.FromBulkString(m));
            if (withScores) results.Add(RespValue.FromBulkString(FormatScore(zset.MemberScores[m])));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> ZUnionStore(CommandContext ctx) => ZStore(ctx, "UNION");
    private static ValueTask<RespValue> ZInterStore(CommandContext ctx) => ZStore(ctx, "INTER");
    private static ValueTask<RespValue> ZDiffStore(CommandContext ctx) => ZStore(ctx, "DIFF");

    private static ValueTask<RespValue> ZStore(CommandContext ctx, string op)
    {
        var destKey = ctx.GetArgString(0);
        var numKeys = (int)ctx.GetArgLong(1);
        var keys = new string[numKeys];
        for (int i = 0; i < numKeys; i++) keys[i] = ctx.GetArgString(2 + i);

        var sets = keys.Select(k => GetZSet(ctx, k)).ToArray();
        var result = new Dictionary<string, double>();

        if (op == "UNION")
        {
            foreach (var zset in sets)
            {
                if (zset == null) continue;
                foreach (var (member, score) in zset.MemberScores)
                    result[member] = result.GetValueOrDefault(member) + score;
            }
        }
        else if (op == "INTER")
        {
            if (sets.Length > 0 && sets[0] != null)
            {
                foreach (var (member, score) in sets[0]!.MemberScores)
                    result[member] = score;
                for (int i = 1; i < sets.Length; i++)
                {
                    if (sets[i] == null) { result.Clear(); break; }
                    foreach (var key in result.Keys.ToArray())
                    {
                        if (sets[i]!.MemberScores.TryGetValue(key, out var s))
                            result[key] += s;
                        else result.Remove(key);
                    }
                }
            }
        }
        else // DIFF
        {
            if (sets.Length > 0 && sets[0] != null)
            {
                foreach (var (member, score) in sets[0]!.MemberScores) result[member] = score;
                for (int i = 1; i < sets.Length; i++)
                {
                    if (sets[i] == null) continue;
                    foreach (var member in sets[i]!.MemberScores.Keys) result.Remove(member);
                }
            }
        }

        if (result.Count == 0) { ctx.Database.RemoveEntry(destKey); return ValueTask.FromResult(RespValue.Zero); }
        var dest = new RedisSortedSet();
        foreach (var (member, score) in result) dest.Add(member, score);
        ctx.Database.SetEntry(destKey, dest);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(result.Count));
    }

    private static ValueTask<RespValue> ZScan(CommandContext ctx)
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
        var zset = GetZSet(ctx, key);
        if (zset == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[] { RespValue.FromBulkString("0"), RespValue.EmptyArray }));

        var members = zset.ScoreIndex
            .Where(e => pattern == null || ServerCommands.MatchesGlob(e.Member, pattern))
            .ToList();
        var start = (int)cursor;
        var end = Math.Min(start + count, members.Count);
        var next = end >= members.Count ? 0 : end;
        var results = new List<RespValue>();
        for (int i = start; i < end; i++)
        {
            results.Add(RespValue.FromBulkString(members[i].Member));
            results.Add(RespValue.FromBulkString(FormatScore(members[i].Score)));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[] { RespValue.FromBulkString(next.ToString()), new RespValue.Array(results.ToArray()) }));
    }

    // Ref: https://redis.io/docs/latest/commands/zrangestore/
    private static ValueTask<RespValue> ZRangeStore(CommandContext ctx)
    {
        var destKey = ctx.GetArgString(0);
        var srcKey = ctx.GetArgString(1);
        var min = ctx.GetArgString(2);
        var max = ctx.GetArgString(3);

        var zset = GetZSet(ctx, srcKey);
        if (zset == null)
        {
            ctx.Database.RemoveEntry(destKey);
            return ValueTask.FromResult(RespValue.Zero);
        }

        bool byScore = false, byLex = false, rev = false;
        int offset = 0, count = int.MaxValue;
        for (int i = 4; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "BYSCORE") byScore = true;
            else if (opt == "BYLEX") byLex = true;
            else if (opt == "REV") rev = true;
            else if (opt == "LIMIT") { i++; offset = (int)long.Parse(ctx.GetArgString(i)); i++; count = (int)long.Parse(ctx.GetArgString(i)); }
        }

        IEnumerable<(double Score, string Member)> items;
        if (byScore)
        {
            var (minV, minExcl) = ParseScoreBound(min, double.NegativeInfinity);
            var (maxV, maxExcl) = ParseScoreBound(max, double.PositiveInfinity);
            items = (rev ? zset.ScoreIndex.Reverse() : zset.ScoreIndex)
                .Where(e => (minExcl ? e.Score > minV : e.Score >= minV) && (maxExcl ? e.Score < maxV : e.Score <= maxV));
        }
        else if (byLex)
        {
            var (minL, minExcl, minInf) = ParseLexBound(min);
            var (maxL, maxExcl, maxInf) = ParseLexBound(max);
            items = (rev ? zset.ScoreIndex.Reverse() : zset.ScoreIndex)
                .Where(e => (minInf || (minExcl ? string.CompareOrdinal(e.Member, minL) > 0 : string.CompareOrdinal(e.Member, minL) >= 0)) &&
                           (maxInf || (maxExcl ? string.CompareOrdinal(e.Member, maxL) < 0 : string.CompareOrdinal(e.Member, maxL) <= 0)));
        }
        else
        {
            var start = int.Parse(min);
            var stop = int.Parse(max);
            var list = (rev ? zset.ScoreIndex.Reverse() : zset.ScoreIndex).ToList();
            var len = list.Count;
            if (start < 0) start = Math.Max(len + start, 0);
            if (stop < 0) stop = len + stop;
            stop = Math.Min(stop, len - 1);
            items = start > stop ? Enumerable.Empty<(double, string)>() : list.Skip(start).Take(stop - start + 1);
        }

        var result = items.Skip(offset).Take(count).ToList();
        if (result.Count == 0)
        {
            ctx.Database.RemoveEntry(destKey);
            return ValueTask.FromResult(RespValue.Zero);
        }

        var dest = new RedisSortedSet();
        foreach (var (score, member) in result) dest.Add(member, score);
        ctx.Database.SetEntry(destKey, dest);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(result.Count));
    }

    private static double ParseScore(string s)
    {
        if (s.Equals("+inf", StringComparison.OrdinalIgnoreCase) || s == "inf") return double.PositiveInfinity;
        if (s.Equals("-inf", StringComparison.OrdinalIgnoreCase)) return double.NegativeInfinity;
        return double.Parse(s, NumberStyles.Float, CultureInfo.InvariantCulture);
    }

    private static (double value, bool exclusive) ParseScoreBound(string s, double defaultInf)
    {
        if (s == "-inf" || s == "+inf" || s == "inf") return (defaultInf == double.NegativeInfinity ? double.NegativeInfinity : double.PositiveInfinity, false);
        if (s.StartsWith('(')) return (double.Parse(s[1..], NumberStyles.Float, CultureInfo.InvariantCulture), true);
        return (double.Parse(s, NumberStyles.Float, CultureInfo.InvariantCulture), false);
    }

    private static (string value, bool exclusive, bool isInf) ParseLexBound(string s)
    {
        if (s == "-") return ("", false, true);
        if (s == "+") return ("", false, true);
        if (s.StartsWith('(')) return (s[1..], true, false);
        if (s.StartsWith('[')) return (s[1..], false, false);
        return (s, false, false);
    }

    private static string FormatScore(double score)
    {
        if (double.IsPositiveInfinity(score)) return "inf";
        if (double.IsNegativeInfinity(score)) return "-inf";
        return score.ToString("G17", CultureInfo.InvariantCulture);
    }
}
