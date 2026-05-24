using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class KeyCommands : ICommandHandler
{
    private readonly InMemoryRedisStore _store;

    public KeyCommands(InMemoryRedisStore store) => _store = store;

    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "DEL" => Del(context),
            "UNLINK" => Del(context), // UNLINK is async DEL, same behavior in emulator
            "EXISTS" => Exists(context),
            "EXPIRE" => Expire(context),
            "PEXPIRE" => PExpire(context),
            "EXPIREAT" => ExpireAt(context),
            "PEXPIREAT" => PExpireAt(context),
            "PERSIST" => Persist(context),
            "TTL" => Ttl(context),
            "PTTL" => PTtl(context),
            "EXPIRETIME" => ExpireTime(context),
            "PEXPIRETIME" => PExpireTime(context),
            "TYPE" => Type(context),
            "RENAME" => Rename(context),
            "RENAMENX" => RenameNx(context),
            "KEYS" => Keys(context),
            "SCAN" => Scan(context),
            "RANDOMKEY" => RandomKey(context),
            "COPY" => Copy(context),
            "TOUCH" => Touch(context),
            "OBJECT" => Object(context),
            "DUMP" => Dump(context),
            "RESTORE" => Restore(context),
            "SORT" => Sort(context),
            "WAIT" => Wait(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static ValueTask<RespValue> Del(CommandContext ctx)
    {
        int count = 0;
        for (int i = 0; i < ctx.Arguments.Length; i++)
            if (ctx.Database.RemoveEntry(ctx.GetArgString(i))) count++;
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private static ValueTask<RespValue> Exists(CommandContext ctx)
    {
        int count = 0;
        for (int i = 0; i < ctx.Arguments.Length; i++)
            if (ctx.Database.KeyExists(ctx.GetArgString(i))) count++;
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private static ValueTask<RespValue> Expire(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var seconds = ctx.GetArgLong(1);
        var options = ParseExpireOptions(ctx, 2);
        return SetExpiryWithOptions(ctx.Database, key, DateTimeOffset.UtcNow.AddSeconds(seconds), options);
    }

    private static ValueTask<RespValue> PExpire(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var ms = ctx.GetArgLong(1);
        var options = ParseExpireOptions(ctx, 2);
        return SetExpiryWithOptions(ctx.Database, key, DateTimeOffset.UtcNow.AddMilliseconds(ms), options);
    }

    private static ValueTask<RespValue> ExpireAt(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var timestamp = ctx.GetArgLong(1);
        var options = ParseExpireOptions(ctx, 2);
        return SetExpiryWithOptions(ctx.Database, key, DateTimeOffset.FromUnixTimeSeconds(timestamp), options);
    }

    private static ValueTask<RespValue> PExpireAt(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var timestamp = ctx.GetArgLong(1);
        var options = ParseExpireOptions(ctx, 2);
        return SetExpiryWithOptions(ctx.Database, key, DateTimeOffset.FromUnixTimeMilliseconds(timestamp), options);
    }

    private static (bool nx, bool xx, bool gt, bool lt) ParseExpireOptions(CommandContext ctx, int startIdx)
    {
        bool nx = false, xx = false, gt = false, lt = false;
        for (int i = startIdx; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "NX": nx = true; break;
                case "XX": xx = true; break;
                case "GT": gt = true; break;
                case "LT": lt = true; break;
            }
        }
        return (nx, xx, gt, lt);
    }

    private static ValueTask<RespValue> SetExpiryWithOptions(RedisDatabase db, string key,
        DateTimeOffset newExpiry, (bool nx, bool xx, bool gt, bool lt) options)
    {
        var entry = db.GetEntry(key);
        if (entry == null)
            return ValueTask.FromResult(RespValue.Zero);

        if (options.nx && entry.Expiry.HasValue)
            return ValueTask.FromResult(RespValue.Zero);
        if (options.xx && !entry.Expiry.HasValue)
            return ValueTask.FromResult(RespValue.Zero);
        if (options.gt && entry.Expiry.HasValue && newExpiry <= entry.Expiry.Value)
            return ValueTask.FromResult(RespValue.Zero);
        if (options.lt)
        {
            if (!entry.Expiry.HasValue || newExpiry >= entry.Expiry.Value)
                return ValueTask.FromResult(RespValue.Zero);
        }

        entry.Expiry = newExpiry;
        return ValueTask.FromResult(RespValue.One);
    }

    private static ValueTask<RespValue> Persist(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetEntry(key);
        if (entry == null || !entry.Expiry.HasValue)
            return ValueTask.FromResult(RespValue.Zero);
        entry.Expiry = null;
        return ValueTask.FromResult(RespValue.One);
    }

    private static ValueTask<RespValue> Ttl(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) return ValueTask.FromResult(RespValue.NegativeTwo);
        if (!entry.Expiry.HasValue) return ValueTask.FromResult(RespValue.NegativeOne);
        var ttl = (long)(entry.Expiry.Value - DateTimeOffset.UtcNow).TotalSeconds;
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(Math.Max(ttl, 0)));
    }

    private static ValueTask<RespValue> PTtl(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) return ValueTask.FromResult(RespValue.NegativeTwo);
        if (!entry.Expiry.HasValue) return ValueTask.FromResult(RespValue.NegativeOne);
        var ttl = (long)(entry.Expiry.Value - DateTimeOffset.UtcNow).TotalMilliseconds;
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(Math.Max(ttl, 0)));
    }

    private static ValueTask<RespValue> ExpireTime(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) return ValueTask.FromResult(RespValue.NegativeTwo);
        if (!entry.Expiry.HasValue) return ValueTask.FromResult(RespValue.NegativeOne);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(entry.Expiry.Value.ToUnixTimeSeconds()));
    }

    private static ValueTask<RespValue> PExpireTime(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) return ValueTask.FromResult(RespValue.NegativeTwo);
        if (!entry.Expiry.HasValue) return ValueTask.FromResult(RespValue.NegativeOne);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(entry.Expiry.Value.ToUnixTimeMilliseconds()));
    }

    private static ValueTask<RespValue> Type(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var typeName = ctx.Database.GetKeyType(key);
        return ValueTask.FromResult<RespValue>(new RespValue.SimpleString(typeName));
    }

    private static ValueTask<RespValue> Rename(CommandContext ctx)
    {
        var src = ctx.GetArgString(0);
        var dst = ctx.GetArgString(1);
        var entry = ctx.Database.GetEntry(src);
        if (entry == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "no such key"));
        ctx.Database.RemoveEntry(src);
        ctx.Database.SetEntry(dst, entry);
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> RenameNx(CommandContext ctx)
    {
        var src = ctx.GetArgString(0);
        var dst = ctx.GetArgString(1);
        var entry = ctx.Database.GetEntry(src);
        if (entry == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "no such key"));
        if (ctx.Database.KeyExists(dst))
            return ValueTask.FromResult(RespValue.Zero);
        ctx.Database.RemoveEntry(src);
        ctx.Database.SetEntry(dst, entry);
        return ValueTask.FromResult(RespValue.One);
    }

    private static ValueTask<RespValue> Keys(CommandContext ctx)
    {
        var pattern = ctx.GetArgString(0);
        var keys = ctx.Database.RawEntries
            .Where(kvp => !kvp.Value.IsExpired && ServerCommands.MatchesGlob(kvp.Key, pattern))
            .Select(kvp => (RespValue)RespValue.FromBulkString(kvp.Key))
            .ToArray();
        return ValueTask.FromResult<RespValue>(new RespValue.Array(keys));
    }

    private static ValueTask<RespValue> Scan(CommandContext ctx)
    {
        var cursor = ctx.GetArgLong(0);
        string? pattern = null;
        int count = 10;
        string? type = null;

        for (int i = 1; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "MATCH": i++; pattern = ctx.GetArgString(i); break;
                case "COUNT": i++; count = (int)ctx.GetArgLong(i); break;
                case "TYPE": i++; type = ctx.GetArgString(i).ToLowerInvariant(); break;
            }
        }

        var allKeys = ctx.Database.RawEntries
            .Where(kvp => !kvp.Value.IsExpired)
            .Where(kvp => pattern == null || ServerCommands.MatchesGlob(kvp.Key, pattern))
            .Where(kvp => type == null || kvp.Value.TypeName == type)
            .Select(kvp => kvp.Key)
            .OrderBy(k => k)
            .ToList();

        var startIdx = (int)cursor;
        var endIdx = Math.Min(startIdx + count, allKeys.Count);
        var nextCursor = endIdx >= allKeys.Count ? 0 : endIdx;

        var keys = allKeys.Skip(startIdx).Take(count)
            .Select(k => (RespValue)RespValue.FromBulkString(k))
            .ToArray();

        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString(nextCursor.ToString()),
            new RespValue.Array(keys)
        }));
    }

    private static ValueTask<RespValue> RandomKey(CommandContext ctx)
    {
        var keys = ctx.Database.RawEntries
            .Where(kvp => !kvp.Value.IsExpired)
            .Select(kvp => kvp.Key)
            .ToArray();
        if (keys.Length == 0)
            return ValueTask.FromResult(RespValue.NullBulkString);
        var key = keys[Random.Shared.Next(keys.Length)];
        return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(key));
    }

    private ValueTask<RespValue> Copy(CommandContext ctx)
    {
        var src = ctx.GetArgString(0);
        var dst = ctx.GetArgString(1);
        bool replace = false;
        int? destDb = null;

        for (int i = 2; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "REPLACE": replace = true; break;
                case "DESTINATION": i++; /* skip, handled by DB */ break;
                case "DB": i++; destDb = (int)ctx.GetArgLong(i); break;
            }
        }

        var entry = ctx.Database.GetEntry(src);
        if (entry == null) return ValueTask.FromResult(RespValue.Zero);

        var targetDb = destDb.HasValue ? _store.GetDatabase(destDb.Value) : ctx.Database;
        if (!replace && targetDb.KeyExists(dst))
            return ValueTask.FromResult(RespValue.Zero);

        targetDb.SetEntry(dst, entry);
        return ValueTask.FromResult(RespValue.One);
    }

    private static ValueTask<RespValue> Touch(CommandContext ctx)
    {
        int count = 0;
        for (int i = 0; i < ctx.Arguments.Length; i++)
            if (ctx.Database.KeyExists(ctx.GetArgString(i))) count++;
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private static ValueTask<RespValue> Object(CommandContext ctx)
    {
        if (ctx.Arguments.Length == 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'object' command"));

        var sub = ctx.GetArgString(0).ToUpperInvariant();
        if (sub == "ENCODING" && ctx.Arguments.Length > 1)
        {
            var key = ctx.GetArgString(1);
            var entry = ctx.Database.GetEntry(key);
            if (entry == null)
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "no such key"));
            var encoding = entry switch
            {
                RedisString => "embstr",
                _ => "raw"
            };
            return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(encoding));
        }
        if (sub == "REFCOUNT" && ctx.Arguments.Length > 1)
            return ValueTask.FromResult<RespValue>(new RespValue.Integer(1));
        if (sub == "IDLETIME" && ctx.Arguments.Length > 1)
            return ValueTask.FromResult<RespValue>(new RespValue.Integer(0));
        if (sub == "FREQ" && ctx.Arguments.Length > 1)
            return ValueTask.FromResult<RespValue>(new RespValue.Integer(0));
        if (sub == "HELP")
            return ValueTask.FromResult(RespValue.EmptyArray);

        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown subcommand '{sub}'"));
    }

    private static ValueTask<RespValue> Dump(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) return ValueTask.FromResult(RespValue.NullBulkString);
        // Simplified: return the raw bytes for strings, placeholder for others
        if (entry is RedisString rs)
            return ValueTask.FromResult<RespValue>(new RespValue.BulkString(rs.Value));
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(Encoding.UTF8.GetBytes($"[{entry.TypeName}]")));
    }

    private static ValueTask<RespValue> Restore(CommandContext ctx)
    {
        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "RESTORE is not fully supported in the emulator"));
    }

    private static ValueTask<RespValue> Sort(CommandContext ctx)
    {
        // Simplified SORT: only supports basic ASC/DESC ALPHA on list/set/zset
        return ValueTask.FromResult(RespValue.EmptyArray);
    }

    private static ValueTask<RespValue> Wait(CommandContext ctx)
    {
        // WAIT is a replication command, always return 0 replicas
        return ValueTask.FromResult(RespValue.Zero);
    }
}
