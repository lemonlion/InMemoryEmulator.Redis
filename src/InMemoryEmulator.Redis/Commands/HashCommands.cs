using System.Globalization;
using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class HashCommands : ICommandHandler
{
    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "HSET" => HSet(context),
            "HGET" => HGet(context),
            "HDEL" => HDel(context),
            "HEXISTS" => HExists(context),
            "HGETALL" => HGetAll(context),
            "HKEYS" => HKeys(context),
            "HVALS" => HVals(context),
            "HLEN" => HLen(context),
            "HMSET" => HMSet(context),
            "HMGET" => HMGet(context),
            "HINCRBY" => HIncrBy(context),
            "HINCRBYFLOAT" => HIncrByFloat(context),
            "HSETNX" => HSetNx(context),
            "HRANDFIELD" => HRandField(context),
            "HSCAN" => HScan(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static RedisHash GetOrCreateHash(CommandContext ctx, string key)
    {
        var entry = ctx.Database.GetEntry(key);
        if (entry == null)
        {
            var h = new RedisHash();
            ctx.Database.SetEntry(key, h);
            return h;
        }
        if (entry is not RedisHash hash) throw new WrongTypeException();
        return hash;
    }

    private static ValueTask<RespValue> HSet(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var hash = GetOrCreateHash(ctx, key);
        int count = 0;
        for (int i = 1; i + 1 < ctx.Arguments.Length; i += 2)
        {
            var field = ctx.GetArgString(i);
            var value = ctx.GetArgBytes(i + 1) ?? Array.Empty<byte>();
            if (!hash.Fields.ContainsKey(field)) count++;
            hash.Fields[field] = value;
        }
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private static ValueTask<RespValue> HGet(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var field = ctx.GetArgString(1);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null || !entry.Fields.TryGetValue(field, out var value))
            return ValueTask.FromResult(RespValue.NullBulkString);
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(value));
    }

    private static ValueTask<RespValue> HDel(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.Zero);
        int count = 0;
        for (int i = 1; i < ctx.Arguments.Length; i++)
            if (entry.Fields.Remove(ctx.GetArgString(i))) count++;
        if (entry.Fields.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private static ValueTask<RespValue> HExists(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var field = ctx.GetArgString(1);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        return ValueTask.FromResult<RespValue>(
            entry != null && entry.Fields.ContainsKey(field) ? RespValue.One : RespValue.Zero);
    }

    private static ValueTask<RespValue> HGetAll(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.EmptyArray);
        var results = new RespValue[entry.Fields.Count * 2];
        int i = 0;
        foreach (var (field, value) in entry.Fields)
        {
            results[i++] = RespValue.FromBulkString(field);
            results[i++] = new RespValue.BulkString(value);
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> HKeys(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.EmptyArray);
        var results = entry.Fields.Keys.Select(f => (RespValue)RespValue.FromBulkString(f)).ToArray();
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> HVals(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.EmptyArray);
        var results = entry.Fields.Values.Select(v => (RespValue)new RespValue.BulkString(v)).ToArray();
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> HLen(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(entry?.Fields.Count ?? 0));
    }

    private static ValueTask<RespValue> HMSet(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var hash = GetOrCreateHash(ctx, key);
        for (int i = 1; i + 1 < ctx.Arguments.Length; i += 2)
        {
            var field = ctx.GetArgString(i);
            var value = ctx.GetArgBytes(i + 1) ?? Array.Empty<byte>();
            hash.Fields[field] = value;
        }
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> HMGet(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        var results = new RespValue[ctx.Arguments.Length - 1];
        for (int i = 1; i < ctx.Arguments.Length; i++)
        {
            var field = ctx.GetArgString(i);
            if (entry != null && entry.Fields.TryGetValue(field, out var value))
                results[i - 1] = new RespValue.BulkString(value);
            else
                results[i - 1] = RespValue.NullBulkString;
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> HIncrBy(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var field = ctx.GetArgString(1);
        var increment = ctx.GetArgLong(2);
        var hash = GetOrCreateHash(ctx, key);

        long current = 0;
        if (hash.Fields.TryGetValue(field, out var existing))
        {
            if (!long.TryParse(Encoding.UTF8.GetString(existing), out current))
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "hash value is not an integer"));
        }
        var newValue = current + increment;
        hash.Fields[field] = Encoding.UTF8.GetBytes(newValue.ToString());
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(newValue));
    }

    private static ValueTask<RespValue> HIncrByFloat(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var field = ctx.GetArgString(1);
        if (!double.TryParse(ctx.GetArgString(2), NumberStyles.Float, CultureInfo.InvariantCulture, out var increment))
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "value is not a valid float"));

        var hash = GetOrCreateHash(ctx, key);
        double current = 0;
        if (hash.Fields.TryGetValue(field, out var existing))
        {
            if (!double.TryParse(Encoding.UTF8.GetString(existing), NumberStyles.Float, CultureInfo.InvariantCulture, out current))
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "hash value is not a float"));
        }
        var newValue = current + increment;
        var formatted = newValue.ToString("G17", CultureInfo.InvariantCulture);
        hash.Fields[field] = Encoding.UTF8.GetBytes(formatted);
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(Encoding.UTF8.GetBytes(formatted)));
    }

    private static ValueTask<RespValue> HSetNx(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var field = ctx.GetArgString(1);
        var value = ctx.GetArgBytes(2) ?? Array.Empty<byte>();
        var hash = GetOrCreateHash(ctx, key);
        if (hash.Fields.ContainsKey(field))
            return ValueTask.FromResult(RespValue.Zero);
        hash.Fields[field] = value;
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult(RespValue.One);
    }

    private static ValueTask<RespValue> HRandField(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null || entry.Fields.Count == 0)
            return ValueTask.FromResult(ctx.Arguments.Length > 1 ? RespValue.EmptyArray : RespValue.NullBulkString);

        int count = ctx.Arguments.Length > 1 ? (int)ctx.GetArgLong(1) : 1;
        bool withValues = ctx.Arguments.Length > 2 && ctx.GetArgString(2).Equals("WITHVALUES", StringComparison.OrdinalIgnoreCase);
        bool single = ctx.Arguments.Length == 1;

        var fields = entry.Fields.Keys.ToArray();
        if (single)
        {
            var f = fields[Random.Shared.Next(fields.Length)];
            return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(f));
        }

        bool allowDuplicates = count < 0;
        count = Math.Abs(count);
        var results = new List<RespValue>();

        for (int i = 0; i < count; i++)
        {
            var f = fields[Random.Shared.Next(fields.Length)];
            results.Add(RespValue.FromBulkString(f));
            if (withValues) results.Add(new RespValue.BulkString(entry.Fields[f]));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> HScan(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var cursor = ctx.GetArgLong(1);
        string? pattern = null;
        int count = 10;
        for (int i = 2; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "MATCH") { i++; pattern = ctx.GetArgString(i); }
            else if (opt == "COUNT") { i++; count = (int)ctx.GetArgLong(i); }
        }

        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
            {
                RespValue.FromBulkString("0"),
                RespValue.EmptyArray
            }));

        var fields = entry.Fields.Keys
            .Where(f => pattern == null || ServerCommands.MatchesGlob(f, pattern))
            .OrderBy(f => f)
            .ToList();

        var startIdx = (int)cursor;
        var endIdx = Math.Min(startIdx + count, fields.Count);
        var nextCursor = endIdx >= fields.Count ? 0 : endIdx;

        var results = new List<RespValue>();
        for (int i = startIdx; i < endIdx; i++)
        {
            results.Add(RespValue.FromBulkString(fields[i]));
            results.Add(new RespValue.BulkString(entry.Fields[fields[i]]));
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString(nextCursor.ToString()),
            new RespValue.Array(results.ToArray())
        }));
    }
}
