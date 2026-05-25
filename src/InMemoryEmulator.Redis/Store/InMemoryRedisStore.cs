using System.Text;
using System.Text.Json;

namespace InMemoryEmulator.Redis.Store;

public sealed class InMemoryRedisStore
{
    public const int DatabaseCount = 16;
    private readonly RedisDatabase[] _databases;

    public InMemoryRedisStore()
    {
        _databases = new RedisDatabase[DatabaseCount];
        for (int i = 0; i < DatabaseCount; i++)
            _databases[i] = new RedisDatabase();
    }

    internal RedisDatabase GetDatabase(int index)
    {
        if (index < 0 || index >= DatabaseCount)
            throw new ArgumentOutOfRangeException(nameof(index));
        return _databases[index];
    }

    // Ref: https://redis.io/docs/latest/commands/swapdb/
    //   "Swaps two Redis databases, so that immediately all the clients connected to a given database
    //    will see the data of the other database, and the other way around."
    internal void SwapDatabases(int index1, int index2)
    {
        if (index1 < 0 || index1 >= DatabaseCount)
            throw new ArgumentOutOfRangeException(nameof(index1));
        if (index2 < 0 || index2 >= DatabaseCount)
            throw new ArgumentOutOfRangeException(nameof(index2));
        (_databases[index1], _databases[index2]) = (_databases[index2], _databases[index1]);
    }

    public void FlushAll()
    {
        for (int i = 0; i < DatabaseCount; i++)
            _databases[i].FlushDb();
    }

    public string ExportState()
    {
        var state = new Dictionary<string, Dictionary<string, object>>();
        for (int i = 0; i < DatabaseCount; i++)
        {
            var db = _databases[i];
            var dbState = new Dictionary<string, object>();
            foreach (var kvp in db.RawEntries)
            {
                if (kvp.Value.IsExpired) continue;
                var entry = kvp.Value;
                var entryData = new Dictionary<string, object>
                {
                    ["type"] = entry.TypeName
                };
                if (entry.Expiry.HasValue)
                    entryData["expiry"] = entry.Expiry.Value.ToUnixTimeMilliseconds();

                switch (entry)
                {
                    case RedisString rs:
                        entryData["value"] = Convert.ToBase64String(rs.Value);
                        break;
                    case RedisHash rh:
                        entryData["fields"] = rh.Fields.ToDictionary(
                            f => f.Key, f => Convert.ToBase64String(f.Value));
                        break;
                    case RedisList rl:
                        entryData["items"] = rl.Items.Select(Convert.ToBase64String).ToList();
                        break;
                    case RedisSet rs:
                        entryData["members"] = rs.Members.ToList();
                        break;
                    case RedisSortedSet rz:
                        entryData["members"] = rz.MemberScores.ToDictionary(m => m.Key, m => m.Value);
                        break;
                }
                dbState[kvp.Key] = entryData;
            }
            if (dbState.Count > 0)
                state[$"db{i}"] = dbState;
        }
        return JsonSerializer.Serialize(state, new JsonSerializerOptions { WriteIndented = true });
    }

    public void ImportState(string json)
    {
        var state = JsonSerializer.Deserialize<Dictionary<string, Dictionary<string, JsonElement>>>(json);
        if (state == null) return;

        FlushAll();
        foreach (var (dbKey, dbState) in state)
        {
            if (!dbKey.StartsWith("db") || !int.TryParse(dbKey[2..], out var dbIndex)) continue;
            if (dbIndex < 0 || dbIndex >= DatabaseCount) continue;
            var db = _databases[dbIndex];

            foreach (var (key, entryJson) in dbState)
            {
                var type = entryJson.GetProperty("type").GetString();
                RedisEntry entry = type switch
                {
                    "string" => new RedisString
                    {
                        Value = Convert.FromBase64String(entryJson.GetProperty("value").GetString()!)
                    },
                    "hash" => CreateHashFromJson(entryJson),
                    "list" => CreateListFromJson(entryJson),
                    "set" => CreateSetFromJson(entryJson),
                    "zset" => CreateSortedSetFromJson(entryJson),
                    _ => new RedisString { Value = Array.Empty<byte>() }
                };

                if (entryJson.TryGetProperty("expiry", out var expiryEl))
                    entry.Expiry = DateTimeOffset.FromUnixTimeMilliseconds(expiryEl.GetInt64());

                db.SetEntry(key, entry);
            }
        }
    }

    private static RedisHash CreateHashFromJson(JsonElement el)
    {
        var hash = new RedisHash();
        if (el.TryGetProperty("fields", out var fields))
        {
            foreach (var prop in fields.EnumerateObject())
                hash.Fields[prop.Name] = Convert.FromBase64String(prop.Value.GetString()!);
        }
        return hash;
    }

    private static RedisList CreateListFromJson(JsonElement el)
    {
        var list = new RedisList();
        if (el.TryGetProperty("items", out var items))
        {
            foreach (var item in items.EnumerateArray())
                list.Items.AddLast(Convert.FromBase64String(item.GetString()!));
        }
        return list;
    }

    private static RedisSet CreateSetFromJson(JsonElement el)
    {
        var set = new RedisSet();
        if (el.TryGetProperty("members", out var members))
        {
            foreach (var m in members.EnumerateArray())
                set.Members.Add(m.GetString()!);
        }
        return set;
    }

    private static RedisSortedSet CreateSortedSetFromJson(JsonElement el)
    {
        var zset = new RedisSortedSet();
        if (el.TryGetProperty("members", out var members))
        {
            foreach (var prop in members.EnumerateObject())
                zset.Add(prop.Name, prop.Value.GetDouble());
        }
        return zset;
    }
}
