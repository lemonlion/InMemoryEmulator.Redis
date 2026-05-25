using System.Text;
using InMemoryEmulator.Redis.Commands;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Server;

internal sealed class CommandRouter
{
    private readonly Dictionary<string, ICommandHandler> _handlers = new(StringComparer.OrdinalIgnoreCase);
    private readonly CommandLog _commandLog;
    private ScriptingCommands? _scripting;

    public CommandRouter(InMemoryRedisStore store, PubSubBroker pubSub, CommandLog commandLog)
    {
        _commandLog = commandLog;
        RegisterHandlers(store, pubSub);
    }

    private void RegisterHandlers(InMemoryRedisStore store, PubSubBroker pubSub)
    {
        var server = new ServerCommands(store);
        var strings = new StringCommands();
        var keys = new KeyCommands(store);
        var hashes = new HashCommands();
        var lists = new ListCommands();
        var sets = new SetCommands();
        var sortedSets = new SortedSetCommands();
        var pubSubCmds = new PubSubCommands(pubSub);
        var transactions = new TransactionCommands(this, store);
        var hll = new HyperLogLogCommands();
        var geo = new GeoCommands();
        var streams = new StreamCommands();
        var bitmaps = new BitmapCommands();
        var scripting = new ScriptingCommands(this);
        _scripting = scripting;

        Register("PING", server);
        Register("ECHO", server);
        Register("INFO", server);
        Register("DBSIZE", server);
        Register("TIME", server);
        Register("SELECT", server);
        Register("COMMAND", server);
        Register("CONFIG", server);
        Register("HELLO", server);
        Register("CLIENT", server);
        Register("CLUSTER", server);
        Register("FLUSHDB", server);
        Register("FLUSHALL", server);
        Register("SWAPDB", server);
        Register("RESET", server);
        Register("AUTH", server);
        Register("QUIT", server);
        Register("SLOWLOG", server);
        Register("MEMORY", server);
        Register("DEBUG", server);

        // String commands
        Register("GET", strings);
        Register("SET", strings);
        Register("GETSET", strings);
        Register("GETDEL", strings);
        Register("GETEX", strings);
        Register("MGET", strings);
        Register("MSET", strings);
        Register("MSETNX", strings);
        Register("SETNX", strings);
        Register("SETEX", strings);
        Register("PSETEX", strings);
        Register("INCR", strings);
        Register("INCRBY", strings);
        Register("INCRBYFLOAT", strings);
        Register("DECR", strings);
        Register("DECRBY", strings);
        Register("APPEND", strings);
        Register("STRLEN", strings);
        Register("GETRANGE", strings);
        Register("SETRANGE", strings);
        Register("SUBSTR", strings);
        Register("LCS", strings);

        // Bitmap commands
        Register("SETBIT", bitmaps);
        Register("GETBIT", bitmaps);
        Register("BITCOUNT", bitmaps);
        Register("BITOP", bitmaps);
        Register("BITPOS", bitmaps);
        Register("BITFIELD", bitmaps);
        Register("BITFIELD_RO", bitmaps);

        // Key commands
        Register("DEL", keys);
        Register("UNLINK", keys);
        Register("EXISTS", keys);
        Register("EXPIRE", keys);
        Register("PEXPIRE", keys);
        Register("EXPIREAT", keys);
        Register("PEXPIREAT", keys);
        Register("PERSIST", keys);
        Register("TTL", keys);
        Register("PTTL", keys);
        Register("EXPIRETIME", keys);
        Register("PEXPIRETIME", keys);
        Register("TYPE", keys);
        Register("RENAME", keys);
        Register("RENAMENX", keys);
        Register("KEYS", keys);
        Register("SCAN", keys);
        Register("RANDOMKEY", keys);
        Register("COPY", keys);
        Register("TOUCH", keys);
        Register("OBJECT", keys);
        Register("DUMP", keys);
        Register("RESTORE", keys);
        Register("SORT", keys);
        Register("SORT_RO", keys);
        Register("WAIT", keys);
        Register("MOVE", keys);

        // Hash commands
        Register("HSET", hashes);
        Register("HGET", hashes);
        Register("HDEL", hashes);
        Register("HEXISTS", hashes);
        Register("HGETALL", hashes);
        Register("HKEYS", hashes);
        Register("HVALS", hashes);
        Register("HLEN", hashes);
        Register("HMSET", hashes);
        Register("HMGET", hashes);
        Register("HINCRBY", hashes);
        Register("HINCRBYFLOAT", hashes);
        Register("HSETNX", hashes);
        Register("HRANDFIELD", hashes);
        Register("HSCAN", hashes);
        Register("HSTRLEN", hashes);
        // Redis 7.4 per-field expiry commands
        Register("HEXPIRE", hashes);
        Register("HPEXPIRE", hashes);
        Register("HEXPIREAT", hashes);
        Register("HPEXPIREAT", hashes);
        Register("HTTL", hashes);
        Register("HPTTL", hashes);
        Register("HEXPIRETIME", hashes);
        Register("HPEXPIRETIME", hashes);
        Register("HPERSIST", hashes);
        Register("HGETDEL", hashes);
        Register("HGETEX", hashes);
        Register("HSETEX", hashes);

        // List commands
        Register("LPUSH", lists);
        Register("RPUSH", lists);
        Register("LPUSHX", lists);
        Register("RPUSHX", lists);
        Register("LPOP", lists);
        Register("RPOP", lists);
        Register("LLEN", lists);
        Register("LRANGE", lists);
        Register("LINDEX", lists);
        Register("LSET", lists);
        Register("LINSERT", lists);
        Register("LREM", lists);
        Register("LTRIM", lists);
        Register("RPOPLPUSH", lists);
        Register("LMOVE", lists);
        Register("LPOS", lists);
        Register("BLPOP", lists);
        Register("BRPOP", lists);
        Register("BLMOVE", lists);
        Register("BLMPOP", lists);
        Register("LMPOP", lists);
        Register("BRPOPLPUSH", lists);

        // Set commands
        Register("SADD", sets);
        Register("SREM", sets);
        Register("SMEMBERS", sets);
        Register("SISMEMBER", sets);
        Register("SMISMEMBER", sets);
        Register("SCARD", sets);
        Register("SPOP", sets);
        Register("SRANDMEMBER", sets);
        Register("SINTER", sets);
        Register("SINTERCARD", sets);
        Register("SINTERSTORE", sets);
        Register("SUNION", sets);
        Register("SUNIONSTORE", sets);
        Register("SDIFF", sets);
        Register("SDIFFSTORE", sets);
        Register("SMOVE", sets);
        Register("SSCAN", sets);

        // Sorted Set commands
        Register("ZADD", sortedSets);
        Register("ZREM", sortedSets);
        Register("ZSCORE", sortedSets);
        Register("ZMSCORE", sortedSets);
        Register("ZRANK", sortedSets);
        Register("ZREVRANK", sortedSets);
        Register("ZCARD", sortedSets);
        Register("ZCOUNT", sortedSets);
        Register("ZINCRBY", sortedSets);
        Register("ZRANGE", sortedSets);
        Register("ZREVRANGE", sortedSets);
        Register("ZRANGEBYSCORE", sortedSets);
        Register("ZREVRANGEBYSCORE", sortedSets);
        Register("ZRANGEBYLEX", sortedSets);
        Register("ZREVRANGEBYLEX", sortedSets);
        Register("ZLEXCOUNT", sortedSets);
        Register("ZPOPMIN", sortedSets);
        Register("ZPOPMAX", sortedSets);
        Register("ZRANDMEMBER", sortedSets);
        Register("ZUNIONSTORE", sortedSets);
        Register("ZINTERSTORE", sortedSets);
        Register("ZDIFFSTORE", sortedSets);
        Register("ZSCAN", sortedSets);
        Register("ZRANGESTORE", sortedSets);
        Register("BZPOPMIN", sortedSets);
        Register("BZPOPMAX", sortedSets);
        Register("ZREMRANGEBYLEX", sortedSets);
        Register("ZREMRANGEBYRANK", sortedSets);
        Register("ZREMRANGEBYSCORE", sortedSets);
        Register("ZDIFF", sortedSets);
        Register("ZUNION", sortedSets);
        Register("ZINTER", sortedSets);
        Register("ZINTERCARD", sortedSets);
        Register("ZMPOP", sortedSets);
        Register("BZMPOP", sortedSets);

        // Pub/Sub commands
        Register("SUBSCRIBE", pubSubCmds);
        Register("UNSUBSCRIBE", pubSubCmds);
        Register("PSUBSCRIBE", pubSubCmds);
        Register("PUNSUBSCRIBE", pubSubCmds);
        Register("PUBLISH", pubSubCmds);
        Register("PUBSUB", pubSubCmds);
        Register("SSUBSCRIBE", pubSubCmds);
        Register("SUNSUBSCRIBE", pubSubCmds);
        Register("SPUBLISH", pubSubCmds);

        // Transaction commands
        Register("MULTI", transactions);
        Register("EXEC", transactions);
        Register("DISCARD", transactions);
        Register("WATCH", transactions);
        Register("UNWATCH", transactions);

        // HyperLogLog commands
        Register("PFADD", hll);
        Register("PFCOUNT", hll);
        Register("PFMERGE", hll);

        // Geo commands
        Register("GEOADD", geo);
        Register("GEODIST", geo);
        Register("GEOHASH", geo);
        Register("GEOPOS", geo);
        Register("GEOSEARCH", geo);
        Register("GEOSEARCHSTORE", geo);
        Register("GEORADIUS", geo);
        Register("GEORADIUS_RO", geo);
        Register("GEORADIUSBYMEMBER", geo);
        Register("GEORADIUSBYMEMBER_RO", geo);

        // Stream commands
        Register("XADD", streams);
        Register("XLEN", streams);
        Register("XRANGE", streams);
        Register("XREVRANGE", streams);
        Register("XREAD", streams);
        Register("XTRIM", streams);
        Register("XDEL", streams);
        Register("XINFO", streams);
        Register("XGROUP", streams);
        Register("XREADGROUP", streams);
        Register("XACK", streams);
        Register("XPENDING", streams);
        Register("XCLAIM", streams);
        Register("XAUTOCLAIM", streams);
        Register("XSETID", streams);

        // Scripting commands
        Register("EVAL", scripting);
        Register("EVALSHA", scripting);
        Register("EVAL_RO", scripting);
        Register("EVALSHA_RO", scripting);
        Register("SCRIPT", scripting);
    }

    public async ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        _commandLog.Record(new CommandRecord
        {
            CommandName = context.CommandName,
            Arguments = context.Arguments.Select(a => a switch
            {
                RespValue.BulkString { Data: { } data } => Encoding.UTF8.GetString(data),
                RespValue.SimpleString { Value: var v } => v,
                RespValue.Integer { Value: var v } => v.ToString(),
                _ => ""
            }).ToArray(),
            Timestamp = DateTimeOffset.UtcNow,
            DatabaseIndex = context.Client.SelectedDatabase
        });

        if (!_handlers.TryGetValue(context.CommandName, out var handler))
            return new RespValue.Error("ERR", $"unknown command '{context.CommandName}', with args beginning with: ");

        try
        {
            return await handler.ExecuteAsync(context);
        }
        catch (WrongTypeException)
        {
            return RespValue.WrongTypeError;
        }
    }

    internal void Register(string name, ICommandHandler handler) => _handlers[name] = handler;

    public void SetLuaEngine(ILuaScriptEngine engine) => _scripting?.SetLuaEngine(engine);
}
