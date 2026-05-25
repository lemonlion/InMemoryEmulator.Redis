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
            "SORT_RO" => Sort(context),
            "WAIT" => Wait(context),
            "MOVE" => Move(context),
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

    // Ref: https://redis.io/docs/latest/commands/dump/
    // Our format: [type_byte][payload]. Not wire-compatible with real Redis RDB but roundtrips within emulator.
    private static ValueTask<RespValue> Dump(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) return ValueTask.FromResult(RespValue.NullBulkString);

        using var ms = new System.IO.MemoryStream();
        using var bw = new System.IO.BinaryWriter(ms);
        bw.Write((byte)(entry switch { RedisString => 0, RedisList => 1, RedisSet => 2, RedisHash => 3, RedisSortedSet => 4, _ => 255 }));

        switch (entry)
        {
            case RedisString rs:
                bw.Write(rs.Value.Length);
                bw.Write(rs.Value);
                break;
            case RedisList rl:
                bw.Write(rl.Items.Count);
                foreach (var item in rl.Items) { bw.Write(item.Length); bw.Write(item); }
                break;
            case RedisSet rs:
                bw.Write(rs.Members.Count);
                foreach (var m in rs.Members) { bw.Write(m); }
                break;
            case RedisHash rh:
                bw.Write(rh.Fields.Count);
                foreach (var (f, v) in rh.Fields) { bw.Write(f); bw.Write(v.Length); bw.Write(v); }
                break;
            case RedisSortedSet rz:
                bw.Write(rz.MemberScores.Count);
                foreach (var (m, s) in rz.MemberScores) { bw.Write(m); bw.Write(s); }
                break;
        }
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(ms.ToArray()));
    }

    // Ref: https://redis.io/docs/latest/commands/restore/
    private static ValueTask<RespValue> Restore(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var ttlMs = ctx.GetArgLong(1);
        var data = ctx.GetArgBytes(2);
        if (data == null || data.Length == 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "DUMP payload version or checksum are wrong"));

        bool replace = false;
        for (int i = 3; i < ctx.Arguments.Length; i++)
            if (ctx.GetArgString(i).Equals("REPLACE", StringComparison.OrdinalIgnoreCase)) replace = true;

        if (!replace && ctx.Database.KeyExists(key))
            return ValueTask.FromResult<RespValue>(new RespValue.Error("BUSYKEY", "Target key name already exists."));

        using var ms = new System.IO.MemoryStream(data);
        using var br = new System.IO.BinaryReader(ms);
        var typeByte = br.ReadByte();

        RedisEntry entry = typeByte switch
        {
            0 => new RedisString { Value = br.ReadBytes(br.ReadInt32()) },
            1 => RestoreList(br),
            2 => RestoreSet(br),
            3 => RestoreHash(br),
            4 => RestoreSortedSet(br),
            _ => new RedisString { Value = Array.Empty<byte>() }
        };

        if (ttlMs > 0)
            entry.Expiry = DateTimeOffset.UtcNow.AddMilliseconds(ttlMs);

        ctx.Database.SetEntry(key, entry);
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static RedisList RestoreList(System.IO.BinaryReader br)
    {
        var list = new RedisList();
        var count = br.ReadInt32();
        for (int i = 0; i < count; i++) list.Items.AddLast(br.ReadBytes(br.ReadInt32()));
        return list;
    }

    private static RedisSet RestoreSet(System.IO.BinaryReader br)
    {
        var set = new RedisSet();
        var count = br.ReadInt32();
        for (int i = 0; i < count; i++) set.Members.Add(br.ReadString());
        return set;
    }

    private static RedisHash RestoreHash(System.IO.BinaryReader br)
    {
        var hash = new RedisHash();
        var count = br.ReadInt32();
        for (int i = 0; i < count; i++) { var f = br.ReadString(); hash.Fields[f] = br.ReadBytes(br.ReadInt32()); }
        return hash;
    }

    private static RedisSortedSet RestoreSortedSet(System.IO.BinaryReader br)
    {
        var zset = new RedisSortedSet();
        var count = br.ReadInt32();
        for (int i = 0; i < count; i++) { var m = br.ReadString(); zset.Add(m, br.ReadDouble()); }
        return zset;
    }

    // Ref: https://redis.io/docs/latest/commands/sort/
    // Ref: https://redis.io/docs/latest/commands/sort/
    //   "BY pattern: sort by external key values via pattern substitution (* → element)"
    //   "GET pattern: retrieve external values; GET # returns the element itself"
    //   "Multiple GET patterns produce multiple values per element in the result"
    //   "BY nosort: skip sorting, just retrieve GET patterns in natural order"
    private ValueTask<RespValue> Sort(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) return ValueTask.FromResult(RespValue.EmptyArray);

        IEnumerable<string> elements;
        if (entry is RedisList rl)
            elements = rl.Items.Select(b => Encoding.UTF8.GetString(b));
        else if (entry is RedisSet rs)
            elements = rs.Members;
        else if (entry is RedisSortedSet rz)
            elements = rz.ScoreIndex.Select(e => e.Member);
        else
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "One or more scores can't be converted into double"));

        bool alpha = false, desc = false;
        int offset = 0, count = int.MaxValue;
        string? storeKey = null;
        string? byPattern = null;
        var getPatterns = new List<string>();

        for (int i = 1; i < ctx.Arguments.Length; i++)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "ALPHA": alpha = true; break;
                case "DESC": desc = true; break;
                case "ASC": desc = false; break;
                case "LIMIT":
                    i++; offset = (int)ctx.GetArgLong(i);
                    i++; count = (int)ctx.GetArgLong(i);
                    break;
                case "STORE": i++; storeKey = ctx.GetArgString(i); break;
                case "BY": i++; byPattern = ctx.GetArgString(i); break;
                case "GET": i++; getPatterns.Add(ctx.GetArgString(i)); break;
            }
        }

        var elementList = elements.ToList();

        // Ref: https://redis.io/docs/latest/commands/sort/
        //   "When BY is specified with a nosort pattern, SORT skips the sorting operation."
        bool noSort = byPattern != null &&
                      byPattern.Equals("nosort", StringComparison.OrdinalIgnoreCase);

        IEnumerable<string> sorted;
        if (noSort)
        {
            sorted = elementList;
        }
        else if (byPattern != null)
        {
            sorted = SortByPattern(elementList, byPattern, ctx.Database, alpha, desc);
        }
        else
        {
            sorted = alpha
                ? (desc ? elementList.OrderByDescending(e => e, StringComparer.Ordinal) : elementList.OrderBy(e => e, StringComparer.Ordinal))
                : (desc ? elementList.OrderByDescending(e => double.TryParse(e, out var d) ? d : 0) : elementList.OrderBy(e => double.TryParse(e, out var d) ? d : 0));
        }

        var result = sorted.Skip(offset).Take(count).ToList();

        if (getPatterns.Count > 0)
        {
            var output = new List<RespValue>();
            foreach (var elem in result)
            {
                foreach (var pattern in getPatterns)
                {
                    output.Add(ResolveGetPattern(elem, pattern, ctx.Database));
                }
            }

            if (storeKey != null)
            {
                var list = new RedisList();
                foreach (var v in output)
                {
                    if (v is RespValue.BulkString { Data: { } data })
                        list.Items.AddLast(data);
                    else
                        list.Items.AddLast(Array.Empty<byte>());
                }
                if (list.Items.Count > 0) ctx.Database.SetEntry(storeKey, list);
                else ctx.Database.RemoveEntry(storeKey);
                return ValueTask.FromResult<RespValue>(new RespValue.Integer(output.Count));
            }

            return ValueTask.FromResult<RespValue>(new RespValue.Array(output.ToArray()));
        }

        if (storeKey != null)
        {
            var list = new RedisList();
            foreach (var item in result) list.Items.AddLast(Encoding.UTF8.GetBytes(item));
            if (list.Items.Count > 0) ctx.Database.SetEntry(storeKey, list);
            else ctx.Database.RemoveEntry(storeKey);
            return ValueTask.FromResult<RespValue>(new RespValue.Integer(result.Count));
        }

        var respResult = result.Select(e => (RespValue)RespValue.FromBulkString(e)).ToArray();
        return ValueTask.FromResult<RespValue>(new RespValue.Array(respResult));
    }

    // Ref: https://redis.io/docs/latest/commands/sort/
    //   "BY weight_*: Replace first * in pattern with element value to form a key.
    //    If key is hash->field (pattern contains ->), look up hash field."
    private IEnumerable<string> SortByPattern(List<string> elements, string pattern,
        RedisDatabase db, bool alpha, bool desc)
    {
        double LookupSortKey(string elem)
        {
            var resolved = ResolvePattern(elem, pattern, db);
            if (resolved == null) return 0;
            return double.TryParse(resolved, out var d) ? d : 0;
        }

        string LookupSortKeyAlpha(string elem)
        {
            return ResolvePattern(elem, pattern, db) ?? "";
        }

        if (alpha)
            return desc
                ? elements.OrderByDescending(LookupSortKeyAlpha, StringComparer.Ordinal)
                : elements.OrderBy(LookupSortKeyAlpha, StringComparer.Ordinal);

        return desc
            ? elements.OrderByDescending(LookupSortKey)
            : elements.OrderBy(LookupSortKey);
    }

    private static string? ResolvePattern(string element, string pattern, RedisDatabase db)
    {
        var hashSep = pattern.IndexOf("->", StringComparison.Ordinal);
        if (hashSep >= 0)
        {
            var keyPattern = pattern[..hashSep];
            var field = pattern[(hashSep + 2)..];
            var resolvedKey = keyPattern.Replace("*", element);
            var hash = db.GetTyped<RedisHash>(resolvedKey);
            if (hash == null || !hash.Fields.TryGetValue(field, out var val)) return null;
            return Encoding.UTF8.GetString(val);
        }

        var resolved = pattern.Replace("*", element);
        var entry = db.GetTyped<RedisString>(resolved);
        return entry == null ? null : Encoding.UTF8.GetString(entry.Value);
    }

    // Ref: https://redis.io/docs/latest/commands/sort/
    //   "GET #: returns the element itself"
    //   "GET pattern: same substitution as BY, returns the looked-up value or nil"
    private static RespValue ResolveGetPattern(string element, string pattern, RedisDatabase db)
    {
        if (pattern == "#")
            return RespValue.FromBulkString(element);

        var value = ResolvePattern(element, pattern, db);
        return value != null ? RespValue.FromBulkString(value) : RespValue.NullBulkString;
    }

    // Ref: https://redis.io/docs/latest/commands/move/
    //   "Move key from the currently selected database to the specified destination database."
    //   Returns 1 if key was moved, 0 if key doesn't exist or target already has the key.
    private ValueTask<RespValue> Move(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var targetDbIndex = (int)ctx.GetArgLong(1);
        if (targetDbIndex < 0 || targetDbIndex >= InMemoryRedisStore.DatabaseCount)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "invalid DB index"));
        if (targetDbIndex == ctx.Client.SelectedDatabase)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "source and destination objects are the same"));

        var entry = ctx.Database.GetEntry(key);
        if (entry == null)
            return ValueTask.FromResult(RespValue.Zero);

        var targetDb = _store.GetDatabase(targetDbIndex);
        if (targetDb.KeyExists(key))
            return ValueTask.FromResult(RespValue.Zero);

        targetDb.SetEntry(key, entry);
        ctx.Database.RemoveEntry(key);
        return ValueTask.FromResult(RespValue.One);
    }

    private static ValueTask<RespValue> Wait(CommandContext ctx)
    {
        // WAIT is a replication command, always return 0 replicas
        return ValueTask.FromResult(RespValue.Zero);
    }
}
