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
            "BZPOPMIN" => BZPopMin(context),
            "BZPOPMAX" => BZPopMax(context),
            "ZREMRANGEBYLEX" => ZRemRangeByLex(context),
            "ZREMRANGEBYRANK" => ZRemRangeByRank(context),
            "ZREMRANGEBYSCORE" => ZRemRangeByScore(context),
            "ZDIFF" => ZDiff(context),
            "ZUNION" => ZUnion(context),
            "ZINTER" => ZInter(context),
            "ZINTERCARD" => ZInterCard(context),
            "ZMPOP" => ZMPop(context),
            "BZMPOP" => BZMPop(context),
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
        if (added > 0) ctx.Database.NotifyKeyChanged(key);
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

    // Ref: https://redis.io/docs/latest/commands/zrank/
    //   "WITHSCORE (Redis 7.2): return [rank, score] array when specified"
    private static ValueTask<RespValue> ZRank(CommandContext ctx)
    {
        var zset = GetZSet(ctx, ctx.GetArgString(0));
        var member = ctx.GetArgString(1);
        bool withScore = ctx.Arguments.Length > 2 && ctx.GetArgString(2).Equals("WITHSCORE", StringComparison.OrdinalIgnoreCase);

        if (zset == null) return ValueTask.FromResult(withScore ? RespValue.NullArray : RespValue.NullBulkString);
        var rank = zset.GetRank(member);
        if (rank == null) return ValueTask.FromResult(withScore ? RespValue.NullArray : RespValue.NullBulkString);

        if (withScore)
        {
            var score = zset.GetScore(member)!.Value;
            return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
            {
                new RespValue.Integer(rank.Value),
                RespValue.FromBulkString(FormatScore(score))
            }));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(rank.Value));
    }

    // Ref: https://redis.io/docs/latest/commands/zrevrank/
    //   "WITHSCORE (Redis 7.2): return [rank, score] array when specified"
    private static ValueTask<RespValue> ZRevRank(CommandContext ctx)
    {
        var zset = GetZSet(ctx, ctx.GetArgString(0));
        var member = ctx.GetArgString(1);
        bool withScore = ctx.Arguments.Length > 2 && ctx.GetArgString(2).Equals("WITHSCORE", StringComparison.OrdinalIgnoreCase);

        if (zset == null) return ValueTask.FromResult(withScore ? RespValue.NullArray : RespValue.NullBulkString);
        var rank = zset.GetRevRank(member);
        if (rank == null) return ValueTask.FromResult(withScore ? RespValue.NullArray : RespValue.NullBulkString);

        if (withScore)
        {
            var score = zset.GetScore(member)!.Value;
            return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
            {
                new RespValue.Integer(rank.Value),
                RespValue.FromBulkString(FormatScore(score))
            }));
        }
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

    // Ref: https://redis.io/docs/latest/commands/zrange/
    //   "ZRANGE key min max [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]"
    //   Unified syntax from Redis 6.2. Without BYSCORE/BYLEX, min/max are ranks (indices).
    private static ValueTask<RespValue> ZRange(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var minArg = ctx.GetArgString(1);
        var maxArg = ctx.GetArgString(2);

        bool byScore = false, byLex = false, rev = false, withScores = false;
        int offset = 0, count = int.MaxValue;

        for (int i = 3; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "BYSCORE") byScore = true;
            else if (opt == "BYLEX") byLex = true;
            else if (opt == "REV") rev = true;
            else if (opt == "WITHSCORES") withScores = true;
            else if (opt == "LIMIT") { i++; offset = (int)long.Parse(ctx.GetArgString(i)); i++; count = (int)long.Parse(ctx.GetArgString(i)); }
        }

        IEnumerable<(double Score, string Member)> items;

        if (byScore)
        {
            // Ref: https://redis.io/docs/latest/commands/zrange/
            //   "When the REV option is given ... the <min> and <max> elements are swapped,
            //    so that <min> is the element with the highest score."
            if (rev)
            {
                // With REV: first arg (minArg) is the higher bound, second arg (maxArg) is the lower bound
                var (highV, highExcl) = ParseScoreBound(minArg, double.PositiveInfinity);
                var (lowV, lowExcl) = ParseScoreBound(maxArg, double.NegativeInfinity);
                items = zset.ScoreIndex.Reverse()
                    .Where(e => (highExcl ? e.Score < highV : e.Score <= highV) &&
                                (lowExcl ? e.Score > lowV : e.Score >= lowV));
            }
            else
            {
                var (minV, minExcl) = ParseScoreBound(minArg, double.NegativeInfinity);
                var (maxV, maxExcl) = ParseScoreBound(maxArg, double.PositiveInfinity);
                items = zset.ScoreIndex
                    .Where(e => (minExcl ? e.Score > minV : e.Score >= minV) &&
                                (maxExcl ? e.Score < maxV : e.Score <= maxV));
            }
            items = items.Skip(offset).Take(count);
        }
        else if (byLex)
        {
            // Ref: https://redis.io/docs/latest/commands/zrange/
            //   "When BYLEX, min/max are lex bounds. All elements must have the same score."
            var (minL, minExcl, minInf) = ParseLexBound(minArg);
            var (maxL, maxExcl, maxInf) = ParseLexBound(maxArg);

            var source = rev ? zset.ScoreIndex.Reverse() : (IEnumerable<(double Score, string Member)>)zset.ScoreIndex;
            if (rev)
            {
                // When REV, min is the higher lex bound, max is the lower lex bound
                items = source
                    .Where(e => (maxInf || (maxExcl ? string.CompareOrdinal(e.Member, maxL) > 0 : string.CompareOrdinal(e.Member, maxL) >= 0)) &&
                                (minInf || (minExcl ? string.CompareOrdinal(e.Member, minL) < 0 : string.CompareOrdinal(e.Member, minL) <= 0)));
            }
            else
            {
                items = source
                    .Where(e => (minInf || (minExcl ? string.CompareOrdinal(e.Member, minL) > 0 : string.CompareOrdinal(e.Member, minL) >= 0)) &&
                                (maxInf || (maxExcl ? string.CompareOrdinal(e.Member, maxL) < 0 : string.CompareOrdinal(e.Member, maxL) <= 0)));
            }
            items = items.Skip(offset).Take(count);
        }
        else
        {
            // Default: by rank (index)
            var start = int.Parse(minArg);
            var stop = int.Parse(maxArg);
            var list = (rev ? zset.ScoreIndex.Reverse() : (IEnumerable<(double Score, string Member)>)zset.ScoreIndex).ToList();
            var len = list.Count;
            if (start < 0) start = Math.Max(len + start, 0);
            if (stop < 0) stop = len + stop;
            if (start > stop || start >= len) return ValueTask.FromResult(RespValue.EmptyArray);
            stop = Math.Min(stop, len - 1);
            items = list.Skip(start).Take(stop - start + 1);
        }

        var results = new List<RespValue>();
        foreach (var (score, member) in items)
        {
            results.Add(RespValue.FromBulkString(member));
            if (withScores) results.Add(RespValue.FromBulkString(FormatScore(score)));
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

    // Ref: https://redis.io/docs/latest/commands/zlexcount/
    //   "Returns the number of elements in the sorted set at key with a value between
    //    min and max. The min and max arguments have the same meaning as described for
    //    ZRANGEBYLEX."
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

    // Ref: https://redis.io/docs/latest/commands/zrandmember/
    //   "If count is positive, return an array of distinct elements."
    //   "If count is negative, the behavior changes and the command is
    //    allowed to return the same element multiple times."
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

        if (count >= 0)
        {
            var shuffled = members.OrderBy(_ => Random.Shared.Next()).Take(Math.Min(count, members.Length)).ToArray();
            foreach (var m in shuffled)
            {
                results.Add(RespValue.FromBulkString(m));
                if (withScores) results.Add(RespValue.FromBulkString(FormatScore(zset.MemberScores[m])));
            }
        }
        else
        {
            var absCount = Math.Abs(count);
            for (int i = 0; i < absCount; i++)
            {
                var m = members[Random.Shared.Next(members.Length)];
                results.Add(RespValue.FromBulkString(m));
                if (withScores) results.Add(RespValue.FromBulkString(FormatScore(zset.MemberScores[m])));
            }
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> ZUnionStore(CommandContext ctx) => ZStore(ctx, "UNION");
    private static ValueTask<RespValue> ZInterStore(CommandContext ctx) => ZStore(ctx, "INTER");
    private static ValueTask<RespValue> ZDiffStore(CommandContext ctx) => ZStore(ctx, "DIFF");

    // Ref: https://redis.io/docs/latest/commands/zunionstore/
    //   "WEIGHTS: multiply each input set's scores by the corresponding weight (default 1)"
    //   "AGGREGATE SUM|MIN|MAX: how to combine scores for members present in multiple sets (default SUM)"
    // Ref: https://redis.io/docs/latest/commands/zinterstore/
    //   Same WEIGHTS and AGGREGATE semantics as ZUNIONSTORE.
    // Ref: https://redis.io/docs/latest/commands/zdiffstore/
    //   "ZDIFF does not support WEIGHTS or AGGREGATE."
    private static ValueTask<RespValue> ZStore(CommandContext ctx, string op)
    {
        var destKey = ctx.GetArgString(0);
        var numKeys = (int)ctx.GetArgLong(1);
        var keys = new string[numKeys];
        for (int i = 0; i < numKeys; i++) keys[i] = ctx.GetArgString(2 + i);

        int argIdx = 2 + numKeys;
        double[] weights = new double[numKeys];
        Array.Fill(weights, 1.0);
        string aggregate = "SUM";

        while (argIdx < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(argIdx).ToUpperInvariant();
            if (opt == "WEIGHTS")
            {
                argIdx++;
                for (int w = 0; w < numKeys && argIdx < ctx.Arguments.Length; w++, argIdx++)
                    weights[w] = double.Parse(ctx.GetArgString(argIdx), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture);
            }
            else if (opt == "AGGREGATE")
            {
                argIdx++;
                aggregate = ctx.GetArgString(argIdx).ToUpperInvariant();
                argIdx++;
            }
            else { argIdx++; }
        }

        var sets = keys.Select(k => GetZSet(ctx, k)).ToArray();
        var result = CombineSets(sets, weights, aggregate, op);

        if (result.Count == 0) { ctx.Database.RemoveEntry(destKey); return ValueTask.FromResult(RespValue.Zero); }
        var dest = new RedisSortedSet();
        foreach (var (member, score) in result) dest.Add(member, score);
        ctx.Database.SetEntry(destKey, dest);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(result.Count));
    }

    /// <summary>
    /// Shared logic for UNION/INTER/DIFF with WEIGHTS and AGGREGATE support.
    /// </summary>
    private static Dictionary<string, double> CombineSets(
        RedisSortedSet?[] sets, double[] weights, string aggregate, string op)
    {
        var result = new Dictionary<string, double>();

        if (op == "UNION")
        {
            for (int i = 0; i < sets.Length; i++)
            {
                if (sets[i] == null) continue;
                foreach (var (member, score) in sets[i]!.MemberScores)
                {
                    var weighted = score * weights[i];
                    if (result.TryGetValue(member, out var existing))
                        result[member] = AggregateScores(existing, weighted, aggregate);
                    else
                        result[member] = weighted;
                }
            }
        }
        else if (op == "INTER")
        {
            if (sets.Length > 0 && sets[0] != null)
            {
                foreach (var (member, score) in sets[0]!.MemberScores)
                    result[member] = score * weights[0];
                for (int i = 1; i < sets.Length; i++)
                {
                    if (sets[i] == null) { result.Clear(); break; }
                    foreach (var key in result.Keys.ToArray())
                    {
                        if (sets[i]!.MemberScores.TryGetValue(key, out var s))
                            result[key] = AggregateScores(result[key], s * weights[i], aggregate);
                        else result.Remove(key);
                    }
                }
            }
        }
        else // DIFF
        {
            // Ref: https://redis.io/docs/latest/commands/zdiff/
            //   "Returns the difference between the first and all successive sorted sets."
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
        return result;
    }

    private static double AggregateScores(double a, double b, string aggregate)
    {
        return aggregate switch
        {
            "MIN" => Math.Min(a, b),
            "MAX" => Math.Max(a, b),
            _ => a + b // SUM (default)
        };
    }

    // Ref: https://redis.io/docs/latest/commands/zremrangebylex/
    //   "Removes all elements in the sorted set stored at key with a value between min and max (lex range)."
    //   "Returns the number of elements removed."
    private static ValueTask<RespValue> ZRemRangeByLex(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.Zero);

        var (min, minExcl, minInf) = ParseLexBound(ctx.GetArgString(1));
        var (max, maxExcl, maxInf) = ParseLexBound(ctx.GetArgString(2));

        var toRemove = zset.ScoreIndex
            .Select(e => e.Member)
            .Where(m => (minInf || (minExcl ? string.CompareOrdinal(m, min) > 0 : string.CompareOrdinal(m, min) >= 0)) &&
                       (maxInf || (maxExcl ? string.CompareOrdinal(m, max) < 0 : string.CompareOrdinal(m, max) <= 0)))
            .ToList();

        foreach (var member in toRemove) zset.Remove(member);

        if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);

        return ValueTask.FromResult<RespValue>(new RespValue.Integer(toRemove.Count));
    }

    // Ref: https://redis.io/docs/latest/commands/zremrangebyrank/
    //   "Removes all elements in the sorted set stored at key with rank between start and stop."
    //   "Both start and stop are 0-based indexes. Can be negative."
    //   "Returns the number of elements removed."
    private static ValueTask<RespValue> ZRemRangeByRank(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.Zero);

        var start = (int)ctx.GetArgLong(1);
        var stop = (int)ctx.GetArgLong(2);
        var items = zset.ScoreIndex.ToList();
        var len = items.Count;

        if (start < 0) start = Math.Max(len + start, 0);
        if (stop < 0) stop = len + stop;
        if (start > stop || start >= len) return ValueTask.FromResult(RespValue.Zero);
        stop = Math.Min(stop, len - 1);

        var toRemove = items.Skip(start).Take(stop - start + 1).Select(e => e.Member).ToList();
        foreach (var member in toRemove) zset.Remove(member);

        if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);

        return ValueTask.FromResult<RespValue>(new RespValue.Integer(toRemove.Count));
    }

    // Ref: https://redis.io/docs/latest/commands/zremrangebyscore/
    //   "Removes all elements in the sorted set stored at key with a score between min and max (inclusive)."
    //   "Returns the number of elements removed."
    private static ValueTask<RespValue> ZRemRangeByScore(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = GetZSet(ctx, key);
        if (zset == null) return ValueTask.FromResult(RespValue.Zero);

        var (min, minExcl) = ParseScoreBound(ctx.GetArgString(1), double.NegativeInfinity);
        var (max, maxExcl) = ParseScoreBound(ctx.GetArgString(2), double.PositiveInfinity);

        var toRemove = zset.ScoreIndex
            .Where(e => (minExcl ? e.Score > min : e.Score >= min) && (maxExcl ? e.Score < max : e.Score <= max))
            .Select(e => e.Member)
            .ToList();

        foreach (var member in toRemove) zset.Remove(member);

        if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);

        return ValueTask.FromResult<RespValue>(new RespValue.Integer(toRemove.Count));
    }

    // Ref: https://redis.io/docs/latest/commands/zdiff/
    //   "ZDIFF numkeys key [key ...] [WITHSCORES]"
    //   "Returns the difference between the first sorted set and all the successive sorted sets."
    private static ValueTask<RespValue> ZDiff(CommandContext ctx)
    {
        var numKeys = (int)ctx.GetArgLong(0);
        var keys = new string[numKeys];
        for (int i = 0; i < numKeys; i++) keys[i] = ctx.GetArgString(1 + i);

        bool withScores = false;
        int argIdx = 1 + numKeys;
        if (argIdx < ctx.Arguments.Length && ctx.GetArgString(argIdx).Equals("WITHSCORES", StringComparison.OrdinalIgnoreCase))
            withScores = true;

        var sets = keys.Select(k => GetZSet(ctx, k)).ToArray();
        var defaultWeights = new double[numKeys];
        Array.Fill(defaultWeights, 1.0);
        var result = CombineSets(sets, defaultWeights, "SUM", "DIFF");

        // Maintain sorted order (by score then member) from the first set
        var ordered = (sets[0]?.ScoreIndex ?? Enumerable.Empty<(double Score, string Member)>())
            .Where(e => result.ContainsKey(e.Member));

        var output = new List<RespValue>();
        foreach (var (score, member) in ordered)
        {
            output.Add(RespValue.FromBulkString(member));
            if (withScores) output.Add(RespValue.FromBulkString(FormatScore(score)));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(output.ToArray()));
    }

    // Ref: https://redis.io/docs/latest/commands/zunion/
    //   "ZUNION numkeys key [key ...] [WEIGHTS w1 w2 ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]"
    //   "Returns the union without storing."
    private static ValueTask<RespValue> ZUnion(CommandContext ctx)
    {
        return ZCombineNoStore(ctx, "UNION");
    }

    // Ref: https://redis.io/docs/latest/commands/zinter/
    //   "ZINTER numkeys key [key ...] [WEIGHTS w1 w2 ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]"
    //   "Returns the intersection without storing."
    private static ValueTask<RespValue> ZInter(CommandContext ctx)
    {
        return ZCombineNoStore(ctx, "INTER");
    }

    /// <summary>
    /// Shared logic for ZUNION/ZINTER (non-store variants).
    /// </summary>
    private static ValueTask<RespValue> ZCombineNoStore(CommandContext ctx, string op)
    {
        var numKeys = (int)ctx.GetArgLong(0);
        var keys = new string[numKeys];
        for (int i = 0; i < numKeys; i++) keys[i] = ctx.GetArgString(1 + i);

        int argIdx = 1 + numKeys;
        double[] weights = new double[numKeys];
        Array.Fill(weights, 1.0);
        string aggregate = "SUM";
        bool withScores = false;

        while (argIdx < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(argIdx).ToUpperInvariant();
            if (opt == "WEIGHTS")
            {
                argIdx++;
                for (int w = 0; w < numKeys && argIdx < ctx.Arguments.Length; w++, argIdx++)
                    weights[w] = double.Parse(ctx.GetArgString(argIdx), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture);
            }
            else if (opt == "AGGREGATE")
            {
                argIdx++;
                aggregate = ctx.GetArgString(argIdx).ToUpperInvariant();
                argIdx++;
            }
            else if (opt == "WITHSCORES")
            {
                withScores = true;
                argIdx++;
            }
            else { argIdx++; }
        }

        var sets = keys.Select(k => GetZSet(ctx, k)).ToArray();
        var result = CombineSets(sets, weights, aggregate, op);

        // Sort by score then member (standard sorted set ordering)
        var ordered = result.OrderBy(kv => kv.Value).ThenBy(kv => kv.Key, StringComparer.Ordinal);

        var output = new List<RespValue>();
        foreach (var (member, score) in ordered)
        {
            output.Add(RespValue.FromBulkString(member));
            if (withScores) output.Add(RespValue.FromBulkString(FormatScore(score)));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(output.ToArray()));
    }

    // Ref: https://redis.io/docs/latest/commands/zintercard/
    //   "ZINTERCARD numkeys key [key ...] [LIMIT limit]"
    //   "Returns the cardinality of the intersection. LIMIT can cap early exit."
    private static ValueTask<RespValue> ZInterCard(CommandContext ctx)
    {
        var numKeys = (int)ctx.GetArgLong(0);
        var keys = new string[numKeys];
        for (int i = 0; i < numKeys; i++) keys[i] = ctx.GetArgString(1 + i);

        long limit = 0; // 0 means no limit
        int argIdx = 1 + numKeys;
        if (argIdx < ctx.Arguments.Length && ctx.GetArgString(argIdx).Equals("LIMIT", StringComparison.OrdinalIgnoreCase))
        {
            argIdx++;
            limit = ctx.GetArgLong(argIdx);
        }

        var sets = keys.Select(k => GetZSet(ctx, k)).ToArray();

        // If any set is null (key doesn't exist), intersection is empty
        if (sets.Any(s => s == null)) return ValueTask.FromResult(RespValue.Zero);

        // Start with the smallest set for efficiency
        var first = sets[0]!;
        long count = 0;

        foreach (var member in first.MemberScores.Keys)
        {
            bool inAll = true;
            for (int i = 1; i < sets.Length; i++)
            {
                if (!sets[i]!.MemberScores.ContainsKey(member))
                {
                    inAll = false;
                    break;
                }
            }
            if (inAll)
            {
                count++;
                if (limit > 0 && count >= limit) break;
            }
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    // Ref: https://redis.io/docs/latest/commands/zmpop/
    //   "ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]"
    //   "Pops one or more elements from the first non-empty sorted set."
    //   "Returns [key, [[member, score], ...]] or nil."
    private static ValueTask<RespValue> ZMPop(CommandContext ctx)
    {
        var numKeys = (int)ctx.GetArgLong(0);
        var keys = new string[numKeys];
        for (int i = 0; i < numKeys; i++) keys[i] = ctx.GetArgString(1 + i);

        int argIdx = 1 + numKeys;
        var direction = ctx.GetArgString(argIdx).ToUpperInvariant(); // MIN or MAX
        argIdx++;

        int popCount = 1;
        if (argIdx < ctx.Arguments.Length && ctx.GetArgString(argIdx).Equals("COUNT", StringComparison.OrdinalIgnoreCase))
        {
            argIdx++;
            popCount = (int)ctx.GetArgLong(argIdx);
        }

        foreach (var key in keys)
        {
            var zset = GetZSet(ctx, key);
            if (zset == null || zset.ScoreIndex.Count == 0) continue;

            var popped = new List<RespValue>();
            for (int i = 0; i < popCount && zset.ScoreIndex.Count > 0; i++)
            {
                var item = direction == "MAX" ? zset.ScoreIndex.Max : zset.ScoreIndex.Min;
                zset.Remove(item.Member);
                popped.Add(new RespValue.Array(new RespValue[]
                {
                    RespValue.FromBulkString(item.Member),
                    RespValue.FromBulkString(FormatScore(item.Score))
                }));
            }

            if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(key);
            else ctx.Database.IncrementVersion(key);

            return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
            {
                RespValue.FromBulkString(key),
                new RespValue.Array(popped.ToArray())
            }));
        }

        return ValueTask.FromResult(RespValue.NullArray);
    }

    // Ref: https://redis.io/docs/latest/commands/bzmpop/
    //   "BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]"
    //   "Blocking variant of ZMPOP. Returns nil on timeout."
    private static async ValueTask<RespValue> BZMPop(CommandContext ctx)
    {
        var timeoutSeconds = double.Parse(ctx.GetArgString(0), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture);
        var numKeys = (int)ctx.GetArgLong(1);
        var keys = new string[numKeys];
        for (int i = 0; i < numKeys; i++) keys[i] = ctx.GetArgString(2 + i);

        int argIdx = 2 + numKeys;
        var direction = ctx.GetArgString(argIdx).ToUpperInvariant(); // MIN or MAX
        argIdx++;

        int popCount = 1;
        if (argIdx < ctx.Arguments.Length && ctx.GetArgString(argIdx).Equals("COUNT", StringComparison.OrdinalIgnoreCase))
        {
            argIdx++;
            popCount = (int)ctx.GetArgLong(argIdx);
        }

        // Try immediately first
        foreach (var key in keys)
        {
            var zset = GetZSet(ctx, key);
            if (zset != null && zset.ScoreIndex.Count > 0)
                return PopFromZSet(ctx, key, zset, direction, popCount);
        }

        // Block and wait
        if (timeoutSeconds == 0) timeoutSeconds = 5;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));
        try
        {
            var readyKey = await ctx.Database.WaitForKeyChangeAsync(keys, cts.Token);
            var zset = GetZSet(ctx, readyKey);
            if (zset != null && zset.ScoreIndex.Count > 0)
                return PopFromZSet(ctx, readyKey, zset, direction, popCount);
        }
        catch (OperationCanceledException) { }

        return RespValue.NullArray;
    }

    /// <summary>
    /// Helper for ZMPOP/BZMPOP: pop elements from a sorted set and format the response.
    /// </summary>
    private static RespValue PopFromZSet(CommandContext ctx, string key, RedisSortedSet zset, string direction, int popCount)
    {
        var popped = new List<RespValue>();
        for (int i = 0; i < popCount && zset.ScoreIndex.Count > 0; i++)
        {
            var item = direction == "MAX" ? zset.ScoreIndex.Max : zset.ScoreIndex.Min;
            zset.Remove(item.Member);
            popped.Add(new RespValue.Array(new RespValue[]
            {
                RespValue.FromBulkString(item.Member),
                RespValue.FromBulkString(FormatScore(item.Score))
            }));
        }

        if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);

        return new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString(key),
            new RespValue.Array(popped.ToArray())
        });
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

    private static async ValueTask<RespValue> BZPopMin(CommandContext ctx)
    {
        var timeoutSeconds = ctx.GetArgDouble(ctx.Arguments.Length - 1);
        var keys = new string[ctx.Arguments.Length - 1];
        for (int i = 0; i < keys.Length; i++) keys[i] = ctx.GetArgString(i);

        foreach (var key in keys)
        {
            var zset = GetZSet(ctx, key);
            if (zset != null && zset.ScoreIndex.Count > 0)
            {
                var item = zset.ScoreIndex.Min;
                zset.Remove(item.Member);
                if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(key);
                else ctx.Database.IncrementVersion(key);
                return new RespValue.Array(new RespValue[] { RespValue.FromBulkString(key), RespValue.FromBulkString(item.Member), RespValue.FromBulkString(FormatScore(item.Score)) });
            }
        }

        if (timeoutSeconds == 0) timeoutSeconds = 5;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));
        try
        {
            var readyKey = await ctx.Database.WaitForKeyChangeAsync(keys, cts.Token);
            var zset = GetZSet(ctx, readyKey);
            if (zset != null && zset.ScoreIndex.Count > 0)
            {
                var item = zset.ScoreIndex.Min;
                zset.Remove(item.Member);
                if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(readyKey);
                else ctx.Database.IncrementVersion(readyKey);
                return new RespValue.Array(new RespValue[] { RespValue.FromBulkString(readyKey), RespValue.FromBulkString(item.Member), RespValue.FromBulkString(FormatScore(item.Score)) });
            }
        }
        catch (OperationCanceledException) { }
        return RespValue.NullArray;
    }

    private static async ValueTask<RespValue> BZPopMax(CommandContext ctx)
    {
        var timeoutSeconds = ctx.GetArgDouble(ctx.Arguments.Length - 1);
        var keys = new string[ctx.Arguments.Length - 1];
        for (int i = 0; i < keys.Length; i++) keys[i] = ctx.GetArgString(i);

        foreach (var key in keys)
        {
            var zset = GetZSet(ctx, key);
            if (zset != null && zset.ScoreIndex.Count > 0)
            {
                var item = zset.ScoreIndex.Max;
                zset.Remove(item.Member);
                if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(key);
                else ctx.Database.IncrementVersion(key);
                return new RespValue.Array(new RespValue[] { RespValue.FromBulkString(key), RespValue.FromBulkString(item.Member), RespValue.FromBulkString(FormatScore(item.Score)) });
            }
        }

        if (timeoutSeconds == 0) timeoutSeconds = 5;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));
        try
        {
            var readyKey = await ctx.Database.WaitForKeyChangeAsync(keys, cts.Token);
            var zset = GetZSet(ctx, readyKey);
            if (zset != null && zset.ScoreIndex.Count > 0)
            {
                var item = zset.ScoreIndex.Max;
                zset.Remove(item.Member);
                if (zset.MemberScores.Count == 0) ctx.Database.RemoveEntry(readyKey);
                else ctx.Database.IncrementVersion(readyKey);
                return new RespValue.Array(new RespValue[] { RespValue.FromBulkString(readyKey), RespValue.FromBulkString(item.Member), RespValue.FromBulkString(FormatScore(item.Score)) });
            }
        }
        catch (OperationCanceledException) { }
        return RespValue.NullArray;
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

    // Ref: https://redis.io/docs/latest/commands/zlexcount/
    //   "Valid start and stop must start with ( or [ to specify whether the range
    //    item is respectively exclusive or inclusive. The special values of + or -
    //    for start and stop have the special meaning of positively infinite and
    //    negatively infinite strings."
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
