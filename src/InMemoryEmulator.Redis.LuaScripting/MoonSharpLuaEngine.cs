using System.Text;
using InMemoryEmulator.Redis.Server;
using MoonSharp.Interpreter;

namespace InMemoryEmulator.Redis.LuaScripting;

internal sealed class MoonSharpLuaEngine : ILuaScriptEngine
{
    public ValueTask<RespValue> EvalAsync(string script, string[] keys, string[] argv,
        Func<string, string[], ValueTask<RespValue>> callRedis)
    {
        var luaScript = new Script(CoreModules.Preset_SoftSandbox);

        // Register KEYS and ARGV tables (1-indexed in Lua)
        var keysTable = new Table(luaScript);
        for (int i = 0; i < keys.Length; i++)
            keysTable[i + 1] = keys[i];
        luaScript.Globals["KEYS"] = keysTable;

        var argvTable = new Table(luaScript);
        for (int i = 0; i < argv.Length; i++)
            argvTable[i + 1] = argv[i];
        luaScript.Globals["ARGV"] = argvTable;

        // Register redis.call() and redis.pcall()
        var redisTable = new Table(luaScript);

        redisTable["call"] = (Func<ScriptExecutionContext, CallbackArguments, DynValue>)((ctx, args) =>
        {
            return CallRedisSync(luaScript, args, callRedis, throwOnError: true);
        });

        redisTable["pcall"] = (Func<ScriptExecutionContext, CallbackArguments, DynValue>)((ctx, args) =>
        {
            return CallRedisSync(luaScript, args, callRedis, throwOnError: false);
        });

        redisTable["status_reply"] = (Func<ScriptExecutionContext, CallbackArguments, DynValue>)((ctx, args) =>
        {
            var table = new Table(luaScript);
            table["ok"] = args[0].CastToString();
            return DynValue.NewTable(table);
        });

        redisTable["error_reply"] = (Func<ScriptExecutionContext, CallbackArguments, DynValue>)((ctx, args) =>
        {
            var table = new Table(luaScript);
            table["err"] = args[0].CastToString();
            return DynValue.NewTable(table);
        });

        redisTable["log"] = (Func<ScriptExecutionContext, CallbackArguments, DynValue>)((_, _) => DynValue.Nil);

        luaScript.Globals["redis"] = redisTable;

        // Execute
        try
        {
            var result = luaScript.DoString(script);
            return ValueTask.FromResult(ConvertToResp(result));
        }
        catch (ScriptRuntimeException ex)
        {
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", ex.DecoratedMessage ?? ex.Message));
        }
        catch (SyntaxErrorException ex)
        {
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"Error compiling script: {ex.DecoratedMessage ?? ex.Message}"));
        }
    }

    private static DynValue CallRedisSync(Script luaScript, CallbackArguments args,
        Func<string, string[], ValueTask<RespValue>> callRedis, bool throwOnError)
    {
        if (args.Count == 0)
            throw new ScriptRuntimeException("Please specify at least one argument for redis.call()");

        var cmd = args[0].CastToString();
        var redisArgs = new string[args.Count - 1];
        for (int i = 1; i < args.Count; i++)
        {
            redisArgs[i - 1] = args[i].Type switch
            {
                DataType.Number => args[i].Number % 1 == 0
                    ? ((long)args[i].Number).ToString()
                    : args[i].Number.ToString(),
                DataType.Boolean => args[i].Boolean ? "1" : "0",
                _ => args[i].CastToString() ?? ""
            };
        }

        var result = callRedis(cmd, redisArgs).AsTask().GetAwaiter().GetResult();

        if (result is RespValue.Error err && throwOnError)
            throw new ScriptRuntimeException($"@{err.Prefix} {err.Message}");

        return ConvertToDynValue(luaScript, result);
    }

    private static DynValue ConvertToDynValue(Script script, RespValue value)
    {
        return value switch
        {
            RespValue.SimpleString s => s.Value == "OK"
                ? DynValue.NewTable(CreateStatusTable(script, s.Value))
                : DynValue.NewString(s.Value),
            RespValue.Error e => DynValue.NewTable(CreateErrorTable(script, $"{e.Prefix} {e.Message}")),
            RespValue.Integer i => DynValue.NewNumber(i.Value),
            RespValue.BulkString { Data: null } => DynValue.False, // Redis nil → Lua false
            RespValue.BulkString b => DynValue.NewString(Encoding.UTF8.GetString(b.Data!)),
            RespValue.Array { Items: null } => DynValue.False,
            RespValue.Array a => ConvertArrayToDynValue(script, a.Items!),
            _ => DynValue.Nil
        };
    }

    private static DynValue ConvertArrayToDynValue(Script script, RespValue[] items)
    {
        var table = new Table(script);
        for (int i = 0; i < items.Length; i++)
            table[i + 1] = ConvertToDynValue(script, items[i]);
        return DynValue.NewTable(table);
    }

    private static Table CreateStatusTable(Script script, string status)
    {
        var table = new Table(script);
        table["ok"] = status;
        return table;
    }

    private static Table CreateErrorTable(Script script, string error)
    {
        var table = new Table(script);
        table["err"] = error;
        return table;
    }

    private static RespValue ConvertToResp(DynValue value)
    {
        return value.Type switch
        {
            DataType.Nil or DataType.Void => RespValue.NullBulkString,
            DataType.Boolean => value.Boolean ? RespValue.One : RespValue.NullBulkString,
            DataType.Number => value.Number % 1 == 0
                ? new RespValue.Integer((long)value.Number)
                : RespValue.FromBulkString(value.Number.ToString()),
            DataType.String => RespValue.FromBulkString(value.String),
            DataType.Table => ConvertTableToResp(value.Table),
            _ => RespValue.NullBulkString
        };
    }

    private static RespValue ConvertTableToResp(Table table)
    {
        // Check for status reply {ok = "..."}
        var ok = table.Get("ok");
        if (ok.Type == DataType.String)
            return new RespValue.SimpleString(ok.String);

        // Check for error reply {err = "..."}
        var err = table.Get("err");
        if (err.Type == DataType.String)
            return new RespValue.Error("ERR", err.String);

        // Array table (1-indexed sequential)
        var items = new List<RespValue>();
        for (int i = 1; ; i++)
        {
            var item = table.Get(i);
            if (item.Type == DataType.Nil) break;
            items.Add(ConvertToResp(item));
        }
        return new RespValue.Array(items.ToArray());
    }
}
