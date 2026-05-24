using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class ServerCommands : ICommandHandler
{
    private readonly InMemoryRedisStore _store;
    private static int _clientIdCounter;

    public ServerCommands(InMemoryRedisStore store) => _store = store;

    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "PING" => Ping(context),
            "ECHO" => Echo(context),
            "INFO" => Info(context),
            "DBSIZE" => DbSize(context),
            "TIME" => Time(context),
            "SELECT" => Select(context),
            "COMMAND" => Command(context),
            "CONFIG" => Config(context),
            "HELLO" => Hello(context),
            "CLIENT" => Client(context),
            "CLUSTER" => Cluster(context),
            "FLUSHDB" => FlushDb(context),
            "FLUSHALL" => FlushAll(context),
            "AUTH" => Auth(context),
            "QUIT" => Quit(context),
            "SWAPDB" => SwapDb(context),
            "RESET" => Reset(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static ValueTask<RespValue> Ping(CommandContext ctx)
    {
        if (ctx.Arguments.Length == 0)
            return ValueTask.FromResult(RespValue.Pong);
        var data = ctx.GetArgBytes(0);
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(data));
    }

    private static ValueTask<RespValue> Echo(CommandContext ctx)
    {
        if (ctx.Arguments.Length == 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'echo' command"));
        var data = ctx.GetArgBytes(0);
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(data));
    }

    private static ValueTask<RespValue> Info(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/info/
        // StackExchange.Redis parses this during connection handshake.
        // Must include redis_version and role:master at minimum.
        var info = $"""
            # Server
            redis_version:7.0.0
            redis_git_sha1:00000000
            redis_git_dirty:0
            redis_build_id:0
            redis_mode:standalone
            os:Windows
            arch_bits:64
            tcp_port:0
            uptime_in_seconds:1
            uptime_in_days:0
            hz:10
            configured_hz:10

            # Clients
            connected_clients:1
            blocked_clients:0

            # Memory
            used_memory:1000000
            used_memory_human:1.00M
            used_memory_peak:1000000
            used_memory_peak_human:1.00M

            # Stats
            total_connections_received:1
            total_commands_processed:1

            # Replication
            role:master
            connected_slaves:0

            # CPU
            used_cpu_sys:0.000000
            used_cpu_user:0.000000

            # Modules

            # Keyspace

            """;
        return ValueTask.FromResult<RespValue>(new RespValue.BulkString(Encoding.UTF8.GetBytes(info)));
    }

    private static ValueTask<RespValue> Config(CommandContext ctx)
    {
        if (ctx.Arguments.Length == 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'config' command"));

        var subcommand = ctx.GetArgString(0).ToUpperInvariant();
        switch (subcommand)
        {
            case "GET":
                if (ctx.Arguments.Length < 2)
                    return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'config|get' command"));
                return ConfigGet(ctx);
            case "SET":
                return ValueTask.FromResult(RespValue.Ok);
            case "RESETSTAT":
                return ValueTask.FromResult(RespValue.Ok);
            case "REWRITE":
                return ValueTask.FromResult(RespValue.Ok);
            default:
                return ValueTask.FromResult(RespValue.Ok);
        }
    }

    private static ValueTask<RespValue> ConfigGet(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/config-get/
        // Returns alternating key-value pairs matching the pattern
        var pattern = ctx.GetArgString(1);
        var results = new List<RespValue>();

        var configValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["timeout"] = "0",
            ["databases"] = "16",
            ["hz"] = "10",
            ["slave-read-only"] = "yes",
            ["replica-read-only"] = "yes",
            ["slowlog-log-slower-than"] = "10000",
            ["slowlog-max-len"] = "128",
            ["bind-source-addr"] = "",
            ["save"] = "",
            ["appendonly"] = "no"
        };

        foreach (var (key, value) in configValues)
        {
            if (MatchesGlob(key, pattern))
            {
                results.Add(new RespValue.BulkString(Encoding.UTF8.GetBytes(key)));
                results.Add(new RespValue.BulkString(Encoding.UTF8.GetBytes(value)));
            }
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    private static ValueTask<RespValue> Hello(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/hello/
        // Negotiate protocol version and return server info
        var clientId = Interlocked.Increment(ref _clientIdCounter);
        ctx.Client.Authenticated = true;

        var result = new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString("server"),
            RespValue.FromBulkString("redis"),
            RespValue.FromBulkString("version"),
            RespValue.FromBulkString("7.0.0"),
            RespValue.FromBulkString("proto"),
            new RespValue.Integer(2),
            RespValue.FromBulkString("id"),
            new RespValue.Integer(clientId),
            RespValue.FromBulkString("mode"),
            RespValue.FromBulkString("standalone"),
            RespValue.FromBulkString("role"),
            RespValue.FromBulkString("master"),
            RespValue.FromBulkString("modules"),
            RespValue.EmptyArray,
        });
        return ValueTask.FromResult<RespValue>(result);
    }

    private static ValueTask<RespValue> Client(CommandContext ctx)
    {
        if (ctx.Arguments.Length == 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'client' command"));

        var subcommand = ctx.GetArgString(0).ToUpperInvariant();
        switch (subcommand)
        {
            case "SETNAME":
                if (ctx.Arguments.Length < 2)
                    return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'client|setname' command"));
                ctx.Client.ClientName = ctx.GetArgString(1);
                return ValueTask.FromResult(RespValue.Ok);
            case "GETNAME":
                return ValueTask.FromResult<RespValue>(ctx.Client.ClientName != null
                    ? RespValue.FromBulkString(ctx.Client.ClientName)
                    : RespValue.NullBulkString);
            case "ID":
                return ValueTask.FromResult<RespValue>(new RespValue.Integer(ctx.Client.Id));
            case "INFO":
                var info = $"id={ctx.Client.Id} name={ctx.Client.ClientName ?? ""}";
                return ValueTask.FromResult<RespValue>(new RespValue.BulkString(Encoding.UTF8.GetBytes(info)));
            case "LIST":
                var list = $"id={ctx.Client.Id} name={ctx.Client.ClientName ?? ""}\n";
                return ValueTask.FromResult<RespValue>(new RespValue.BulkString(Encoding.UTF8.GetBytes(list)));
            case "NO-EVICT":
            case "NO-TOUCH":
                return ValueTask.FromResult(RespValue.Ok);
            default:
                return ValueTask.FromResult(RespValue.Ok);
        }
    }

    private static ValueTask<RespValue> Command(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/command/
        if (ctx.Arguments.Length == 0)
            return ValueTask.FromResult(RespValue.EmptyArray);

        var sub = ctx.GetArgString(0).ToUpperInvariant();
        return sub switch
        {
            "COUNT" => ValueTask.FromResult<RespValue>(new RespValue.Integer(200)),
            "DOCS" => ValueTask.FromResult(RespValue.EmptyArray),
            "INFO" => ValueTask.FromResult(RespValue.EmptyArray),
            "LIST" => ValueTask.FromResult(RespValue.EmptyArray),
            _ => ValueTask.FromResult(RespValue.EmptyArray)
        };
    }

    private static ValueTask<RespValue> Cluster(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/cluster-info/
        var sub = ctx.Arguments.Length > 0 ? ctx.GetArgString(0).ToUpperInvariant() : "INFO";
        if (sub == "INFO")
        {
            var clusterInfo = "cluster_enabled:0\r\ncluster_state:ok\r\ncluster_slots_assigned:0\r\ncluster_slots_ok:0\r\ncluster_known_nodes:0\r\ncluster_size:0\r\n";
            return ValueTask.FromResult<RespValue>(new RespValue.BulkString(Encoding.UTF8.GetBytes(clusterInfo)));
        }
        if (sub == "MYID")
        {
            return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(""));
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "This instance has cluster support disabled"));
    }

    private ValueTask<RespValue> Select(CommandContext ctx)
    {
        if (ctx.Arguments.Length == 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'select' command"));
        var db = (int)ctx.GetArgLong(0);
        if (db < 0 || db >= InMemoryRedisStore.DatabaseCount)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "DB index is out of range"));
        ctx.Client.SelectedDatabase = db;
        return ValueTask.FromResult(RespValue.Ok);
    }

    private ValueTask<RespValue> DbSize(CommandContext ctx)
    {
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(ctx.Database.Count));
    }

    private static ValueTask<RespValue> Time(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/time/
        var now = DateTimeOffset.UtcNow;
        var seconds = now.ToUnixTimeSeconds();
        var microseconds = now.Microsecond + now.Millisecond * 1000;
        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString(seconds.ToString()),
            RespValue.FromBulkString(microseconds.ToString()),
        }));
    }

    private ValueTask<RespValue> FlushDb(CommandContext ctx)
    {
        ctx.Database.FlushDb();
        return ValueTask.FromResult(RespValue.Ok);
    }

    private ValueTask<RespValue> FlushAll(CommandContext ctx)
    {
        _store.FlushAll();
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> Auth(CommandContext ctx)
    {
        // AUTH is handled at the server level before command routing
        // If we get here, authentication succeeded or isn't required
        ctx.Client.Authenticated = true;
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> Quit(CommandContext ctx)
    {
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> SwapDb(CommandContext ctx)
    {
        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "SWAPDB is not supported in the emulator"));
    }

    private static ValueTask<RespValue> Reset(CommandContext ctx)
    {
        ctx.Client.SelectedDatabase = 0;
        ctx.Client.InTransaction = false;
        ctx.Client.TransactionQueue = null;
        ctx.Client.WatchedKeyVersions = null;
        ctx.Client.ChannelSubscriptions.Clear();
        ctx.Client.PatternSubscriptions.Clear();
        return ValueTask.FromResult<RespValue>(new RespValue.SimpleString("RESET"));
    }

    internal static bool MatchesGlob(string input, string pattern)
    {
        if (pattern == "*") return true;
        return MatchesGlobRecursive(input, 0, pattern, 0);
    }

    private static bool MatchesGlobRecursive(string input, int iIdx, string pattern, int pIdx)
    {
        while (pIdx < pattern.Length)
        {
            if (pattern[pIdx] == '*')
            {
                pIdx++;
                if (pIdx == pattern.Length) return true;
                for (int i = iIdx; i <= input.Length; i++)
                {
                    if (MatchesGlobRecursive(input, i, pattern, pIdx))
                        return true;
                }
                return false;
            }
            if (iIdx >= input.Length) return false;
            if (pattern[pIdx] == '?')
            {
                iIdx++;
                pIdx++;
            }
            else if (pattern[pIdx] == '[')
            {
                pIdx++;
                bool negate = pIdx < pattern.Length && pattern[pIdx] == '^';
                if (negate) pIdx++;
                bool matched = false;
                while (pIdx < pattern.Length && pattern[pIdx] != ']')
                {
                    if (pIdx + 2 < pattern.Length && pattern[pIdx + 1] == '-')
                    {
                        if (input[iIdx] >= pattern[pIdx] && input[iIdx] <= pattern[pIdx + 2])
                            matched = true;
                        pIdx += 3;
                    }
                    else
                    {
                        if (input[iIdx] == pattern[pIdx])
                            matched = true;
                        pIdx++;
                    }
                }
                if (pIdx < pattern.Length) pIdx++; // skip ']'
                if (negate ? matched : !matched) return false;
                iIdx++;
            }
            else if (pattern[pIdx] == '\\' && pIdx + 1 < pattern.Length)
            {
                pIdx++;
                if (input[iIdx] != pattern[pIdx]) return false;
                iIdx++;
                pIdx++;
            }
            else
            {
                if (input[iIdx] != pattern[pIdx]) return false;
                iIdx++;
                pIdx++;
            }
        }
        return iIdx == input.Length;
    }
}
