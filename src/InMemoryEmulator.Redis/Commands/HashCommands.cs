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
            "HSTRLEN" => HStrLen(context),
            // Redis 7.4 per-field expiry commands
            "HEXPIRE" => HExpire(context),
            "HPEXPIRE" => HPExpire(context),
            "HEXPIREAT" => HExpireAt(context),
            "HPEXPIREAT" => HPExpireAt(context),
            "HTTL" => HTtl(context),
            "HPTTL" => HPTtl(context),
            "HEXPIRETIME" => HExpireTime(context),
            "HPEXPIRETIME" => HPExpireTime(context),
            "HPERSIST" => HPersist(context),
            "HGETDEL" => HGetDel(context),
            "HGETEX" => HGetEx(context),
            "HSETEX" => HSetEx(context),
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
            // Ref: https://redis.io/docs/latest/commands/hset/
            //   "If field already exists, its value is overwritten."
            // Ref: https://redis.io/docs/latest/develop/data-types/hashes/#field-expiration
            //   "If a field with an active TTL is overwritten by HSET or equivalent commands,
            //    the existing TTL is removed and the field becomes persistent again."
            if (!hash.FieldExists(field)) count++;
            hash.Fields[field] = value;
            hash.FieldExpiry.Remove(field);
        }
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private static ValueTask<RespValue> HGet(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var field = ctx.GetArgString(1);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.NullBulkString);
        // Use GetField to check per-field expiry
        var value = entry.GetField(field);
        if (value == null)
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
        {
            var field = ctx.GetArgString(i);
            // Check if field is expired first (lazy cleanup)
            entry.IsFieldExpired(field);
            if (entry.Fields.Remove(field))
            {
                entry.FieldExpiry.Remove(field);
                count++;
            }
        }
        if (entry.Fields.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private static ValueTask<RespValue> HExists(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var field = ctx.GetArgString(1);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        // Use FieldExists to check per-field expiry
        return ValueTask.FromResult<RespValue>(
            entry != null && entry.FieldExists(field) ? RespValue.One : RespValue.Zero);
    }

    private static ValueTask<RespValue> HGetAll(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.EmptyArray);
        // Use GetActiveFields to filter out expired fields
        var activeFields = entry.GetActiveFields();
        if (activeFields.Count == 0) return ValueTask.FromResult(RespValue.EmptyArray);
        var results = new RespValue[activeFields.Count * 2];
        int i = 0;
        foreach (var field in activeFields)
        {
            results[i++] = RespValue.FromBulkString(field);
            results[i++] = new RespValue.BulkString(entry.Fields[field]);
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> HKeys(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.EmptyArray);
        // Use GetActiveFields to filter out expired fields
        var activeFields = entry.GetActiveFields();
        var results = activeFields.Select(f => (RespValue)RespValue.FromBulkString(f)).ToArray();
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> HVals(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.EmptyArray);
        // Use GetActiveFields to filter out expired fields
        var activeFields = entry.GetActiveFields();
        var results = activeFields.Select(f => (RespValue)new RespValue.BulkString(entry.Fields[f])).ToArray();
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> HLen(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        // Use ActiveFieldCount to exclude expired fields
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(entry?.ActiveFieldCount ?? 0));
    }

    private static ValueTask<RespValue> HMSet(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var hash = GetOrCreateHash(ctx, key);
        for (int i = 1; i + 1 < ctx.Arguments.Length; i += 2)
        {
            var field = ctx.GetArgString(i);
            var value = ctx.GetArgBytes(i + 1) ?? Array.Empty<byte>();
            // Ref: https://redis.io/docs/latest/develop/data-types/hashes/#field-expiration
            //   "If a field with an active TTL is overwritten by HSET or equivalent commands
            //    (like HMSET or HSETEX without options), the existing TTL is removed."
            hash.Fields[field] = value;
            hash.FieldExpiry.Remove(field);
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
            byte[]? value = null;
            if (entry != null)
                value = entry.GetField(field); // expiry-aware
            if (value != null)
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
        // Use GetField for expiry-aware access
        var existing = hash.GetField(field);
        if (existing != null)
        {
            if (!long.TryParse(Encoding.UTF8.GetString(existing), out current))
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "hash value is not an integer"));
        }
        var newValue = current + increment;
        hash.Fields[field] = Encoding.UTF8.GetBytes(newValue.ToString());
        // Ref: https://redis.io/docs/latest/commands/hincrby/
        //   Incrementing a field clears its per-field expiry (field is recreated).
        hash.FieldExpiry.Remove(field);
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
        // Use GetField for expiry-aware access
        var existing = hash.GetField(field);
        if (existing != null)
        {
            if (!double.TryParse(Encoding.UTF8.GetString(existing), NumberStyles.Float, CultureInfo.InvariantCulture, out current))
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "hash value is not a float"));
        }
        var newValue = current + increment;
        var formatted = newValue.ToString("G17", CultureInfo.InvariantCulture);
        hash.Fields[field] = Encoding.UTF8.GetBytes(formatted);
        hash.FieldExpiry.Remove(field);
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(Encoding.UTF8.GetBytes(formatted)));
    }

    private static ValueTask<RespValue> HSetNx(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var field = ctx.GetArgString(1);
        var value = ctx.GetArgBytes(2) ?? Array.Empty<byte>();
        var hash = GetOrCreateHash(ctx, key);
        // Use FieldExists for expiry-aware check
        if (hash.FieldExists(field))
            return ValueTask.FromResult(RespValue.Zero);
        hash.Fields[field] = value;
        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult(RespValue.One);
    }

    private static ValueTask<RespValue> HRandField(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null || entry.ActiveFieldCount == 0)
            return ValueTask.FromResult(ctx.Arguments.Length > 1 ? RespValue.EmptyArray : RespValue.NullBulkString);

        int count = ctx.Arguments.Length > 1 ? (int)ctx.GetArgLong(1) : 1;
        bool withValues = ctx.Arguments.Length > 2 && ctx.GetArgString(2).Equals("WITHVALUES", StringComparison.OrdinalIgnoreCase);
        bool single = ctx.Arguments.Length == 1;

        var fields = entry.GetActiveFields().ToArray();
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

        // Use GetActiveFields for expiry-aware iteration
        var fields = entry.GetActiveFields()
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

    private static ValueTask<RespValue> HStrLen(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/hstrlen/
        var key = ctx.GetArgString(0);
        var field = ctx.GetArgString(1);
        var entry = ctx.Database.GetTyped<RedisHash>(key);
        if (entry == null) return ValueTask.FromResult(RespValue.Zero);
        // Use GetField for expiry-aware access
        var value = entry.GetField(field);
        if (value == null) return ValueTask.FromResult(RespValue.Zero);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(value.Length));
    }

    // ============================================================
    // Redis 7.4 per-field expiry commands
    // ============================================================

    /// <summary>
    /// Parses the FIELDS numfields field [field ...] portion of per-field expiry commands.
    /// Returns the starting index of "FIELDS" keyword and the list of field names.
    /// </summary>
    private static (int fieldsKeywordIndex, List<string> fields)? ParseFieldsArg(CommandContext ctx, int startSearchIndex)
    {
        // Find the FIELDS keyword
        for (int i = startSearchIndex; i < ctx.Arguments.Length; i++)
        {
            if (ctx.GetArgString(i).Equals("FIELDS", StringComparison.OrdinalIgnoreCase))
            {
                int numFields = (int)ctx.GetArgLong(i + 1);
                var fields = new List<string>(numFields);
                for (int j = 0; j < numFields; j++)
                {
                    fields.Add(ctx.GetArgString(i + 2 + j));
                }
                return (i, fields);
            }
        }
        return null;
    }

    /// <summary>
    /// Parses optional NX/XX/GT/LT condition flags for HEXPIRE-family commands.
    /// Ref: https://redis.io/docs/latest/commands/hexpire/
    /// </summary>
    private static string? ParseConditionFlag(CommandContext ctx, int startIndex, int fieldsKeywordIndex)
    {
        for (int i = startIndex; i < fieldsKeywordIndex; i++)
        {
            var arg = ctx.GetArgString(i).ToUpperInvariant();
            if (arg is "NX" or "XX" or "GT" or "LT")
                return arg;
        }
        return null;
    }

    /// <summary>
    /// Evaluates whether the expiry should be set based on the condition flag.
    /// Ref: https://redis.io/docs/latest/commands/hexpire/
    ///   NX -- Set expiry only when the field has no expiry
    ///   XX -- Set expiry only when the field has an existing expiry
    ///   GT -- Set expiry only when the new expiry is greater than current one
    ///   LT -- Set expiry only when the new expiry is less than current one (or field has no expiry)
    /// </summary>
    private static bool ShouldSetExpiry(RedisHash hash, string field, DateTimeOffset newExpiry, string? condition)
    {
        if (condition == null) return true;

        bool hasExpiry = hash.FieldExpiry.TryGetValue(field, out var currentExpiry);

        return condition switch
        {
            "NX" => !hasExpiry,
            "XX" => hasExpiry,
            "GT" => !hasExpiry || newExpiry > currentExpiry,
            "LT" => !hasExpiry || newExpiry < currentExpiry,
            _ => true
        };
    }

    // Ref: https://redis.io/docs/latest/commands/hexpire/
    //   "Set an expiration (TTL) on one or more fields of a given hash key."
    //   Returns an array of integers for each field:
    //   -2 = no such field in the hash, or key does not exist
    //    0 = specified NX | XX | GT | LT condition not met
    //    1 = expiration time was set or updated
    //    2 = field deleted because the specified expiration is in the past
    private static ValueTask<RespValue> HExpire(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var seconds = ctx.GetArgLong(1);
        var expiry = DateTimeOffset.UtcNow.AddSeconds(seconds);
        return SetFieldExpiry(ctx, key, expiry, 2);
    }

    // Ref: https://redis.io/docs/latest/commands/hpexpire/
    //   Same as HEXPIRE but TTL specified in milliseconds.
    private static ValueTask<RespValue> HPExpire(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var milliseconds = ctx.GetArgLong(1);
        var expiry = DateTimeOffset.UtcNow.AddMilliseconds(milliseconds);
        return SetFieldExpiry(ctx, key, expiry, 2);
    }

    // Ref: https://redis.io/docs/latest/commands/hexpireat/
    //   "Set an expiration (TTL) on one or more fields using an absolute Unix timestamp (seconds)."
    private static ValueTask<RespValue> HExpireAt(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var unixSeconds = ctx.GetArgLong(1);
        var expiry = DateTimeOffset.FromUnixTimeSeconds(unixSeconds);
        return SetFieldExpiry(ctx, key, expiry, 2);
    }

    // Ref: https://redis.io/docs/latest/commands/hpexpireat/
    //   "Set an expiration (TTL) on one or more fields using an absolute Unix timestamp (milliseconds)."
    private static ValueTask<RespValue> HPExpireAt(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var unixMillis = ctx.GetArgLong(1);
        var expiry = DateTimeOffset.FromUnixTimeMilliseconds(unixMillis);
        return SetFieldExpiry(ctx, key, expiry, 2);
    }

    /// <summary>
    /// Shared implementation for HEXPIRE, HPEXPIRE, HEXPIREAT, HPEXPIREAT.
    /// </summary>
    private static ValueTask<RespValue> SetFieldExpiry(CommandContext ctx, string key, DateTimeOffset expiry, int argOffsetAfterValue)
    {
        var entry = ctx.Database.GetTyped<RedisHash>(key);

        // Parse fields argument
        var parsed = ParseFieldsArg(ctx, argOffsetAfterValue);
        if (parsed == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "syntax error"));

        var (fieldsKeywordIndex, fields) = parsed.Value;
        var condition = ParseConditionFlag(ctx, argOffsetAfterValue, fieldsKeywordIndex);

        // Ref: https://redis.io/docs/latest/commands/hexpire/
        //   When called on a key that doesn't exist, returns -2 for each field.
        if (entry == null)
        {
            var noKeyResults = fields.Select(_ => (RespValue)RespValue.NegativeTwo).ToArray();
            return ValueTask.FromResult<RespValue>(new RespValue.Array(noKeyResults));
        }

        var results = new RespValue[fields.Count];
        for (int i = 0; i < fields.Count; i++)
        {
            var field = fields[i];
            // Check if field is expired (lazy cleanup)
            entry.IsFieldExpired(field);

            if (!entry.Fields.ContainsKey(field))
            {
                // -2: no such field
                results[i] = RespValue.NegativeTwo;
            }
            else if (expiry <= DateTimeOffset.UtcNow)
            {
                // Ref: https://redis.io/docs/latest/commands/hexpire/
                //   2: field deleted because the specified expiration is in the past
                if (ShouldSetExpiry(entry, field, expiry, condition))
                {
                    entry.Fields.Remove(field);
                    entry.FieldExpiry.Remove(field);
                    results[i] = new RespValue.Integer(2);
                }
                else
                {
                    results[i] = RespValue.Zero;
                }
            }
            else if (!ShouldSetExpiry(entry, field, expiry, condition))
            {
                // 0: condition not met
                results[i] = RespValue.Zero;
            }
            else
            {
                // 1: expiry set/updated
                entry.FieldExpiry[field] = expiry;
                results[i] = RespValue.One;
            }
        }

        // Clean up empty hash
        if (entry.Fields.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    // Ref: https://redis.io/docs/latest/commands/httl/
    //   "Returns the remaining TTL (time to live) of a hash key's field(s) in seconds."
    //   -2 = no such field (or key doesn't exist)
    //   -1 = field has no associated expiry
    //   >= 0 = TTL in seconds
    private static ValueTask<RespValue> HTtl(CommandContext ctx)
    {
        return GetFieldTtl(ctx, isMillis: false, isAbsolute: false);
    }

    // Ref: https://redis.io/docs/latest/commands/hpttl/
    //   Same as HTTL but in milliseconds.
    private static ValueTask<RespValue> HPTtl(CommandContext ctx)
    {
        return GetFieldTtl(ctx, isMillis: true, isAbsolute: false);
    }

    // Ref: https://redis.io/docs/latest/commands/hexpiretime/
    //   "Returns the absolute Unix timestamp (since January 1, 1970) in seconds at which the given key's field(s) will expire."
    private static ValueTask<RespValue> HExpireTime(CommandContext ctx)
    {
        return GetFieldTtl(ctx, isMillis: false, isAbsolute: true);
    }

    // Ref: https://redis.io/docs/latest/commands/hpexpiretime/
    //   Same as HEXPIRETIME but in milliseconds.
    private static ValueTask<RespValue> HPExpireTime(CommandContext ctx)
    {
        return GetFieldTtl(ctx, isMillis: true, isAbsolute: true);
    }

    /// <summary>
    /// Shared implementation for HTTL, HPTTL, HEXPIRETIME, HPEXPIRETIME.
    /// </summary>
    private static ValueTask<RespValue> GetFieldTtl(CommandContext ctx, bool isMillis, bool isAbsolute)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);

        var parsed = ParseFieldsArg(ctx, 1);
        if (parsed == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "syntax error"));

        var (_, fields) = parsed.Value;

        if (entry == null)
        {
            var noKeyResults = fields.Select(_ => (RespValue)RespValue.NegativeTwo).ToArray();
            return ValueTask.FromResult<RespValue>(new RespValue.Array(noKeyResults));
        }

        var results = new RespValue[fields.Count];
        for (int i = 0; i < fields.Count; i++)
        {
            var field = fields[i];
            entry.IsFieldExpired(field);

            if (!entry.Fields.ContainsKey(field))
            {
                results[i] = RespValue.NegativeTwo;
            }
            else if (!entry.FieldExpiry.TryGetValue(field, out var expiry))
            {
                results[i] = RespValue.NegativeOne;
            }
            else
            {
                if (isAbsolute)
                {
                    long timestamp = isMillis
                        ? expiry.ToUnixTimeMilliseconds()
                        : expiry.ToUnixTimeSeconds();
                    results[i] = new RespValue.Integer(timestamp);
                }
                else
                {
                    var remaining = expiry - DateTimeOffset.UtcNow;
                    long ttl = isMillis
                        ? (long)remaining.TotalMilliseconds
                        : (long)remaining.TotalSeconds;
                    results[i] = new RespValue.Integer(Math.Max(0, ttl));
                }
            }
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    // Ref: https://redis.io/docs/latest/commands/hpersist/
    //   "Remove the existing expiration on a hash key's field(s)."
    //   Returns array of integers per field:
    //   -2 = no such field (or key doesn't exist)
    //   -1 = field exists but has no associated expiry
    //    1 = expiry was successfully removed
    private static ValueTask<RespValue> HPersist(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);

        var parsed = ParseFieldsArg(ctx, 1);
        if (parsed == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "syntax error"));

        var (_, fields) = parsed.Value;

        if (entry == null)
        {
            var noKeyResults = fields.Select(_ => (RespValue)RespValue.NegativeTwo).ToArray();
            return ValueTask.FromResult<RespValue>(new RespValue.Array(noKeyResults));
        }

        var results = new RespValue[fields.Count];
        for (int i = 0; i < fields.Count; i++)
        {
            var field = fields[i];
            entry.IsFieldExpired(field);

            if (!entry.Fields.ContainsKey(field))
            {
                results[i] = RespValue.NegativeTwo;
            }
            else if (!entry.FieldExpiry.ContainsKey(field))
            {
                results[i] = RespValue.NegativeOne;
            }
            else
            {
                entry.FieldExpiry.Remove(field);
                results[i] = RespValue.One;
            }
        }

        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    // Ref: https://redis.io/docs/latest/commands/hgetdel/
    //   "Get the value of one or more fields and delete those fields."
    //   Returns array of bulk strings (nil for fields that don't exist).
    private static ValueTask<RespValue> HGetDel(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);

        var parsed = ParseFieldsArg(ctx, 1);
        if (parsed == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "syntax error"));

        var (_, fields) = parsed.Value;

        if (entry == null)
        {
            var nullResults = fields.Select(_ => RespValue.NullBulkString).ToArray();
            return ValueTask.FromResult<RespValue>(new RespValue.Array(nullResults));
        }

        var results = new RespValue[fields.Count];
        for (int i = 0; i < fields.Count; i++)
        {
            var field = fields[i];
            var value = entry.GetField(field);
            if (value != null)
            {
                results[i] = new RespValue.BulkString(value);
                entry.Fields.Remove(field);
                entry.FieldExpiry.Remove(field);
            }
            else
            {
                results[i] = RespValue.NullBulkString;
            }
        }

        if (entry.Fields.Count == 0) ctx.Database.RemoveEntry(key);
        else ctx.Database.IncrementVersion(key);

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    // Ref: https://redis.io/docs/latest/commands/hgetex/
    //   "Get the value of one or more fields of a given hash key, and optionally set their expiration."
    //   Syntax: HGETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST] FIELDS numfields field [field ...]
    //   Returns array of bulk strings (nil for fields that don't exist).
    private static ValueTask<RespValue> HGetEx(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisHash>(key);

        // Parse the expiry option and FIELDS keyword
        DateTimeOffset? expiry = null;
        bool persist = false;
        int searchStart = 1;

        // Look for EX/PX/EXAT/PXAT/PERSIST before FIELDS keyword
        if (ctx.Arguments.Length > 1)
        {
            var opt = ctx.GetArgString(1).ToUpperInvariant();
            switch (opt)
            {
                case "EX":
                    expiry = DateTimeOffset.UtcNow.AddSeconds(ctx.GetArgLong(2));
                    searchStart = 3;
                    break;
                case "PX":
                    expiry = DateTimeOffset.UtcNow.AddMilliseconds(ctx.GetArgLong(2));
                    searchStart = 3;
                    break;
                case "EXAT":
                    expiry = DateTimeOffset.FromUnixTimeSeconds(ctx.GetArgLong(2));
                    searchStart = 3;
                    break;
                case "PXAT":
                    expiry = DateTimeOffset.FromUnixTimeMilliseconds(ctx.GetArgLong(2));
                    searchStart = 3;
                    break;
                case "PERSIST":
                    persist = true;
                    searchStart = 2;
                    break;
            }
        }

        var parsed = ParseFieldsArg(ctx, searchStart);
        if (parsed == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "syntax error"));

        var (_, fields) = parsed.Value;

        if (entry == null)
        {
            var nullResults = fields.Select(_ => RespValue.NullBulkString).ToArray();
            return ValueTask.FromResult<RespValue>(new RespValue.Array(nullResults));
        }

        var results = new RespValue[fields.Count];
        for (int i = 0; i < fields.Count; i++)
        {
            var field = fields[i];
            var value = entry.GetField(field);
            if (value != null)
            {
                results[i] = new RespValue.BulkString(value);
                // Apply expiry modification
                if (persist)
                {
                    entry.FieldExpiry.Remove(field);
                }
                else if (expiry.HasValue)
                {
                    entry.FieldExpiry[field] = expiry.Value;
                }
            }
            else
            {
                results[i] = RespValue.NullBulkString;
            }
        }

        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    // Ref: https://redis.io/docs/latest/commands/hsetex/
    //   "Set the value and optionally the expiration of one or more fields."
    //   Syntax: HSETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds] numfields field value [field value ...]
    //   Returns the number of new fields added (same as HSET).
    private static ValueTask<RespValue> HSetEx(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);

        // Parse the expiry option
        DateTimeOffset? expiry = null;
        int fieldCountIndex = 1;

        if (ctx.Arguments.Length > 1)
        {
            var opt = ctx.GetArgString(1).ToUpperInvariant();
            switch (opt)
            {
                case "EX":
                    expiry = DateTimeOffset.UtcNow.AddSeconds(ctx.GetArgLong(2));
                    fieldCountIndex = 3;
                    break;
                case "PX":
                    expiry = DateTimeOffset.UtcNow.AddMilliseconds(ctx.GetArgLong(2));
                    fieldCountIndex = 3;
                    break;
                case "EXAT":
                    expiry = DateTimeOffset.FromUnixTimeSeconds(ctx.GetArgLong(2));
                    fieldCountIndex = 3;
                    break;
                case "PXAT":
                    expiry = DateTimeOffset.FromUnixTimeMilliseconds(ctx.GetArgLong(2));
                    fieldCountIndex = 3;
                    break;
            }
        }

        var numFields = (int)ctx.GetArgLong(fieldCountIndex);
        var hash = GetOrCreateHash(ctx, key);

        int fieldsStart = fieldCountIndex + 1;
        int count = 0;
        for (int i = 0; i < numFields; i++)
        {
            var field = ctx.GetArgString(fieldsStart + i * 2);
            var value = ctx.GetArgBytes(fieldsStart + i * 2 + 1) ?? Array.Empty<byte>();

            if (!hash.FieldExists(field)) count++;
            hash.Fields[field] = value;

            if (expiry.HasValue)
            {
                hash.FieldExpiry[field] = expiry.Value;
            }
            else
            {
                // Ref: https://redis.io/docs/latest/commands/hsetex/
                //   Without an expiry option, the fields are set without expiry.
                hash.FieldExpiry.Remove(field);
            }
        }

        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }
}
