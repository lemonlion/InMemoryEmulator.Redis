using System.Globalization;
using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class StringCommands : ICommandHandler
{
    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "GET" => Get(context),
            "SET" => Set(context),
            "GETSET" => GetSet(context),
            "GETDEL" => GetDel(context),
            "GETEX" => GetEx(context),
            "MGET" => MGet(context),
            "MSET" => MSet(context),
            "MSETNX" => MSetNx(context),
            "SETNX" => SetNx(context),
            "SETEX" => SetEx(context),
            "PSETEX" => PSetEx(context),
            "INCR" => Incr(context),
            "INCRBY" => IncrBy(context),
            "INCRBYFLOAT" => IncrByFloat(context),
            "DECR" => Decr(context),
            "DECRBY" => DecrBy(context),
            "APPEND" => Append(context),
            "STRLEN" => StrLen(context),
            "GETRANGE" => GetRange(context),
            "SETRANGE" => SetRange(context),
            "SUBSTR" => GetRange(context),
            "LCS" => Lcs(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static ValueTask<RespValue> Get(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisString>(key);
        if (entry == null)
            return ValueTask.FromResult(RespValue.NullBulkString);
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(entry.Value));
    }

    private static ValueTask<RespValue> Set(CommandContext ctx)
    {
        if (ctx.Arguments.Length < 2)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'set' command"));

        var key = ctx.GetArgString(0);
        var value = ctx.GetArgBytes(1) ?? Array.Empty<byte>();

        bool nx = false, xx = false, keepTtl = false, getOld = false;
        TimeSpan? expiry = null;
        DateTimeOffset? expiryAt = null;

        for (int i = 2; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "NX": nx = true; break;
                case "XX": xx = true; break;
                case "KEEPTTL": keepTtl = true; break;
                case "GET": getOld = true; break;
                case "EX":
                    i++;
                    expiry = TimeSpan.FromSeconds(ctx.GetArgLong(i));
                    break;
                case "PX":
                    i++;
                    expiry = TimeSpan.FromMilliseconds(ctx.GetArgLong(i));
                    break;
                case "EXAT":
                    i++;
                    expiryAt = DateTimeOffset.FromUnixTimeSeconds(ctx.GetArgLong(i));
                    break;
                case "PXAT":
                    i++;
                    expiryAt = DateTimeOffset.FromUnixTimeMilliseconds(ctx.GetArgLong(i));
                    break;
            }
        }

        var existing = ctx.Database.GetEntry(key);
        RespValue? oldValue = null;

        if (getOld)
        {
            if (existing is RedisString rs)
                oldValue = new RespValue.BulkString(rs.Value);
            else
                oldValue = RespValue.NullBulkString;
        }

        if (nx && existing != null)
            return ValueTask.FromResult(getOld ? oldValue! : RespValue.NullBulkString);
        if (xx && existing == null)
            return ValueTask.FromResult(getOld ? oldValue! : RespValue.NullBulkString);

        var entry = new RedisString { Value = value };

        if (keepTtl && existing?.Expiry != null)
            entry.Expiry = existing.Expiry;
        else if (expiryAt.HasValue)
            entry.Expiry = expiryAt.Value;
        else if (expiry.HasValue)
            entry.Expiry = DateTimeOffset.UtcNow + expiry.Value;

        ctx.Database.SetEntry(key, entry);

        return ValueTask.FromResult(getOld ? oldValue! : RespValue.Ok);
    }

    private static ValueTask<RespValue> GetSet(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var value = ctx.GetArgBytes(1) ?? Array.Empty<byte>();
        var existing = ctx.Database.GetTyped<RedisString>(key);
        var old = existing != null ? new RespValue.BulkString(existing.Value) : RespValue.NullBulkString;
        ctx.Database.SetEntry(key, new RedisString { Value = value });
        return ValueTask.FromResult<RespValue>(old);
    }

    private static ValueTask<RespValue> GetDel(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var existing = ctx.Database.GetTyped<RedisString>(key);
        if (existing == null)
            return ValueTask.FromResult(RespValue.NullBulkString);
        ctx.Database.RemoveEntry(key);
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(existing.Value));
    }

    private static ValueTask<RespValue> GetEx(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var existing = ctx.Database.GetTyped<RedisString>(key);
        if (existing == null)
            return ValueTask.FromResult(RespValue.NullBulkString);

        for (int i = 1; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "EX":
                    i++;
                    existing.Expiry = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(ctx.GetArgLong(i));
                    break;
                case "PX":
                    i++;
                    existing.Expiry = DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(ctx.GetArgLong(i));
                    break;
                case "EXAT":
                    i++;
                    existing.Expiry = DateTimeOffset.FromUnixTimeSeconds(ctx.GetArgLong(i));
                    break;
                case "PXAT":
                    i++;
                    existing.Expiry = DateTimeOffset.FromUnixTimeMilliseconds(ctx.GetArgLong(i));
                    break;
                case "PERSIST":
                    existing.Expiry = null;
                    break;
            }
        }

        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(existing.Value));
    }

    private static ValueTask<RespValue> MGet(CommandContext ctx)
    {
        var results = new RespValue[ctx.Arguments.Length];
        for (int i = 0; i < ctx.Arguments.Length; i++)
        {
            var key = ctx.GetArgString(i);
            var entry = ctx.Database.GetEntry(key);
            results[i] = entry is RedisString rs ? new RespValue.BulkString(rs.Value) : RespValue.NullBulkString;
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> MSet(CommandContext ctx)
    {
        if (ctx.Arguments.Length % 2 != 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'mset' command"));
        for (int i = 0; i < ctx.Arguments.Length; i += 2)
        {
            var key = ctx.GetArgString(i);
            var value = ctx.GetArgBytes(i + 1) ?? Array.Empty<byte>();
            ctx.Database.SetEntry(key, new RedisString { Value = value });
        }
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> MSetNx(CommandContext ctx)
    {
        if (ctx.Arguments.Length % 2 != 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'msetnx' command"));
        // Check if any key exists
        for (int i = 0; i < ctx.Arguments.Length; i += 2)
        {
            if (ctx.Database.KeyExists(ctx.GetArgString(i)))
                return ValueTask.FromResult(RespValue.Zero);
        }
        // Set all
        for (int i = 0; i < ctx.Arguments.Length; i += 2)
        {
            var key = ctx.GetArgString(i);
            var value = ctx.GetArgBytes(i + 1) ?? Array.Empty<byte>();
            ctx.Database.SetEntry(key, new RedisString { Value = value });
        }
        return ValueTask.FromResult(RespValue.One);
    }

    private static ValueTask<RespValue> SetNx(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var value = ctx.GetArgBytes(1) ?? Array.Empty<byte>();
        if (ctx.Database.KeyExists(key))
            return ValueTask.FromResult(RespValue.Zero);
        ctx.Database.SetEntry(key, new RedisString { Value = value });
        return ValueTask.FromResult(RespValue.One);
    }

    private static ValueTask<RespValue> SetEx(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var seconds = ctx.GetArgLong(1);
        var value = ctx.GetArgBytes(2) ?? Array.Empty<byte>();
        ctx.Database.SetEntry(key, new RedisString
        {
            Value = value,
            Expiry = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(seconds)
        });
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> PSetEx(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var ms = ctx.GetArgLong(1);
        var value = ctx.GetArgBytes(2) ?? Array.Empty<byte>();
        ctx.Database.SetEntry(key, new RedisString
        {
            Value = value,
            Expiry = DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(ms)
        });
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> Incr(CommandContext ctx) => IncrByAmount(ctx, 1);
    private static ValueTask<RespValue> Decr(CommandContext ctx) => IncrByAmount(ctx, -1);

    private static ValueTask<RespValue> IncrBy(CommandContext ctx)
    {
        var amount = ctx.GetArgLong(1);
        return IncrByAmount(ctx, amount);
    }

    private static ValueTask<RespValue> DecrBy(CommandContext ctx)
    {
        var amount = ctx.GetArgLong(1);
        return IncrByAmount(ctx, -amount);
    }

    private static ValueTask<RespValue> IncrByAmount(CommandContext ctx, long amount)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetEntry(key);

        long current = 0;
        if (entry is RedisString rs)
        {
            var str = Encoding.UTF8.GetString(rs.Value);
            if (!long.TryParse(str, out current))
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "value is not an integer or out of range"));
        }
        else if (entry != null)
        {
            return ValueTask.FromResult(RespValue.WrongTypeError);
        }

        var newValue = current + amount;
        var newEntry = entry as RedisString ?? new RedisString();
        newEntry.Value = Encoding.UTF8.GetBytes(newValue.ToString());
        ctx.Database.SetEntry(key, newEntry);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(newValue));
    }

    private static ValueTask<RespValue> IncrByFloat(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var incrementStr = ctx.GetArgString(1);
        if (!double.TryParse(incrementStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var increment))
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "value is not a valid float"));

        var entry = ctx.Database.GetEntry(key);
        double current = 0;
        if (entry is RedisString rs)
        {
            var str = Encoding.UTF8.GetString(rs.Value);
            if (!double.TryParse(str, NumberStyles.Float, CultureInfo.InvariantCulture, out current))
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "value is not a valid float"));
        }
        else if (entry != null)
        {
            return ValueTask.FromResult(RespValue.WrongTypeError);
        }

        var newValue = current + increment;
        var formatted = FormatRedisFloat(newValue);
        var newEntry = entry as RedisString ?? new RedisString();
        newEntry.Value = Encoding.UTF8.GetBytes(formatted);
        ctx.Database.SetEntry(key, newEntry);
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(Encoding.UTF8.GetBytes(formatted)));
    }

    private static ValueTask<RespValue> Append(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var appendValue = ctx.GetArgBytes(1) ?? Array.Empty<byte>();
        var entry = ctx.Database.GetEntry(key);

        if (entry == null)
        {
            ctx.Database.SetEntry(key, new RedisString { Value = appendValue });
            return ValueTask.FromResult<RespValue>(new RespValue.Integer(appendValue.Length));
        }

        if (entry is not RedisString rs)
            return ValueTask.FromResult(RespValue.WrongTypeError);

        var newValue = new byte[rs.Value.Length + appendValue.Length];
        rs.Value.CopyTo(newValue, 0);
        appendValue.CopyTo(newValue, rs.Value.Length);
        rs.Value = newValue;
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(newValue.Length));
    }

    private static ValueTask<RespValue> StrLen(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisString>(key);
        if (entry == null)
            return ValueTask.FromResult(RespValue.Zero);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(entry.Value.Length));
    }

    private static ValueTask<RespValue> GetRange(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var start = (int)ctx.GetArgLong(1);
        var end = (int)ctx.GetArgLong(2);
        var entry = ctx.Database.GetTyped<RedisString>(key);
        if (entry == null || entry.Value.Length == 0)
            return ValueTask.FromResult<RespValue>(new RespValue.BulkString(Array.Empty<byte>()));

        var len = entry.Value.Length;
        if (start < 0) start = Math.Max(len + start, 0);
        if (end < 0) end = len + end;
        if (start > end || start >= len)
            return ValueTask.FromResult<RespValue>(new RespValue.BulkString(Array.Empty<byte>()));

        end = Math.Min(end, len - 1);
        var slice = entry.Value.AsSpan(start, end - start + 1).ToArray();
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(slice));
    }

    private static ValueTask<RespValue> SetRange(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var offset = (int)ctx.GetArgLong(1);
        var value = ctx.GetArgBytes(2) ?? Array.Empty<byte>();

        var entry = ctx.Database.GetEntry(key);
        byte[] current;
        if (entry is RedisString rs)
            current = rs.Value;
        else if (entry == null)
            current = Array.Empty<byte>();
        else
            return ValueTask.FromResult(RespValue.WrongTypeError);

        var newLen = Math.Max(current.Length, offset + value.Length);
        var newValue = new byte[newLen];
        current.CopyTo(newValue, 0);
        value.CopyTo(newValue, offset);

        var newEntry = new RedisString { Value = newValue };
        if (entry?.Expiry != null) newEntry.Expiry = entry.Expiry;
        ctx.Database.SetEntry(key, newEntry);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(newLen));
    }

    // Ref: https://redis.io/docs/latest/commands/lcs/
    //   "Returns the longest common subsequence between two string values."
    private static ValueTask<RespValue> Lcs(CommandContext ctx)
    {
        var key1 = ctx.GetArgString(0);
        var key2 = ctx.GetArgString(1);
        var s1 = ctx.Database.GetTyped<RedisString>(key1);
        var s2 = ctx.Database.GetTyped<RedisString>(key2);
        var str1 = s1 != null ? Encoding.UTF8.GetString(s1.Value) : "";
        var str2 = s2 != null ? Encoding.UTF8.GetString(s2.Value) : "";

        bool wantLen = false;
        bool wantIdx = false;
        long minMatchLen = 0;
        for (int i = 2; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "LEN") wantLen = true;
            else if (opt == "IDX") wantIdx = true;
            else if (opt == "MINMATCHLEN") { i++; minMatchLen = ctx.GetArgLong(i); }
            else if (opt == "WITHMATCHLEN") { /* used with IDX */ }
        }

        var lcs = ComputeLcs(str1, str2);

        if (wantLen)
            return ValueTask.FromResult<RespValue>(new RespValue.Integer(lcs.Length));

        if (wantIdx)
        {
            // Simplified: return matches array and length
            return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
            {
                RespValue.FromBulkString("matches"),
                RespValue.EmptyArray,
                RespValue.FromBulkString("len"),
                new RespValue.Integer(lcs.Length)
            }));
        }

        return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(lcs));
    }

    private static string ComputeLcs(string a, string b)
    {
        var m = a.Length;
        var n = b.Length;
        var dp = new int[m + 1, n + 1];

        for (int i = 1; i <= m; i++)
            for (int j = 1; j <= n; j++)
                dp[i, j] = a[i - 1] == b[j - 1] ? dp[i - 1, j - 1] + 1 : Math.Max(dp[i - 1, j], dp[i, j - 1]);

        var result = new char[dp[m, n]];
        int idx = result.Length - 1;
        int x = m, y = n;
        while (x > 0 && y > 0)
        {
            if (a[x - 1] == b[y - 1]) { result[idx--] = a[x - 1]; x--; y--; }
            else if (dp[x - 1, y] > dp[x, y - 1]) x--;
            else y--;
        }
        return new string(result);
    }

    // Ref: https://redis.io/docs/latest/commands/incrbyfloat/
    //   "The value is stored as a string representation of a floating point number ...
    //    an integer number followed (when needed) by a dot, and a variable number of
    //    digits representing the decimal part of the number. Trailing zeroes are always removed."
    //   Redis uses snprintf with "%.*Lg" (long double, 17 significant digits) then strips
    //   trailing zeros after the decimal point and removes the decimal point if no fractional part remains.
    private static string FormatRedisFloat(double value)
    {
        if (double.IsInfinity(value)) return value > 0 ? "inf" : "-inf";

        // Use G17 for full precision, matching Redis's 17-significant-digit representation
        var s = value.ToString("G17", CultureInfo.InvariantCulture);

        // Strip trailing zeros after the decimal point, matching Redis behavior
        if (s.Contains('.') && !s.Contains('E') && !s.Contains('e'))
        {
            s = s.TrimEnd('0').TrimEnd('.');
        }

        return s;
    }
}
