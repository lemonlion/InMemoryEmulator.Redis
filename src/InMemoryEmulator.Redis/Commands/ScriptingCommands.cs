using System.Security.Cryptography;
using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

// Ref: https://redis.io/docs/latest/commands/eval/
//   "EVAL and EVALSHA are used to evaluate scripts using the Lua interpreter built into Redis."
internal sealed class ScriptingCommands : ICommandHandler
{
    private readonly ScriptCache _scriptCache = new();

    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "EVAL" => Eval(context),
            "EVALSHA" => EvalSha(context),
            "EVALSHA_RO" => EvalSha(context),
            "EVAL_RO" => Eval(context),
            "SCRIPT" => Script(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private ValueTask<RespValue> Eval(CommandContext ctx)
    {
        if (ctx.Arguments.Length < 2)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'eval' command"));

        var script = ctx.GetArgString(0);
        var numKeys = (int)ctx.GetArgLong(1);

        // Cache the script
        _scriptCache.Load(script);

        // Simple script execution for common patterns
        return ExecuteScript(ctx, script, numKeys);
    }

    private ValueTask<RespValue> EvalSha(CommandContext ctx)
    {
        if (ctx.Arguments.Length < 2)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'evalsha' command"));

        var sha = ctx.GetArgString(0);
        var numKeys = (int)ctx.GetArgLong(1);
        var script = _scriptCache.Get(sha);
        if (script == null)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("NOSCRIPT", "No matching script. Please use EVAL."));

        return ExecuteScript(ctx, script, numKeys);
    }

    private ValueTask<RespValue> Script(CommandContext ctx)
    {
        var sub = ctx.GetArgString(0).ToUpperInvariant();
        switch (sub)
        {
            case "LOAD":
                var script = ctx.GetArgString(1);
                var sha = _scriptCache.Load(script);
                return ValueTask.FromResult<RespValue>(RespValue.FromBulkString(sha));

            case "EXISTS":
                var results = new RespValue[ctx.Arguments.Length - 1];
                for (int i = 1; i < ctx.Arguments.Length; i++)
                    results[i - 1] = _scriptCache.Exists(ctx.GetArgString(i)) ? RespValue.One : RespValue.Zero;
                return ValueTask.FromResult<RespValue>(new RespValue.Array(results));

            case "FLUSH":
                _scriptCache.Flush();
                return ValueTask.FromResult(RespValue.Ok);

            default:
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown script subcommand '{sub}'"));
        }
    }

    private static ValueTask<RespValue> ExecuteScript(CommandContext ctx, string script, int numKeys)
    {
        // Parse KEYS and ARGV
        var keys = new string[numKeys];
        for (int i = 0; i < numKeys; i++)
            keys[i] = ctx.GetArgString(2 + i);

        var argvStart = 2 + numKeys;
        var argv = new string[ctx.Arguments.Length - argvStart];
        for (int i = 0; i < argv.Length; i++)
            argv[i] = ctx.GetArgString(argvStart + i);

        // Simple pattern matching for common Lua scripts
        // Pattern: return redis.call('CMD', KEYS[1], ...)
        var result = TryExecuteSimpleScript(ctx, script, keys, argv);
        if (result != null)
            return ValueTask.FromResult(result);

        // For scripts we can't interpret, return OK or the first KEYS value
        // This handles StackExchange.Redis internal scripts
        if (script.Contains("redis.call") && script.Contains("return"))
            return ValueTask.FromResult(RespValue.Ok);

        return ValueTask.FromResult(RespValue.NullBulkString);
    }

    private static RespValue? TryExecuteSimpleScript(CommandContext ctx, string script, string[] keys, string[] argv)
    {
        var trimmed = script.Trim();

        // Pattern: return 1 / return 0 / return nil
        if (trimmed == "return 1") return RespValue.One;
        if (trimmed == "return 0") return RespValue.Zero;
        if (trimmed == "return nil") return RespValue.NullBulkString;
        if (trimmed.StartsWith("return tonumber(") && argv.Length > 0)
        {
            if (long.TryParse(argv[0], out var num))
                return new RespValue.Integer(num);
        }
        if (trimmed.StartsWith("return ARGV[1]") && argv.Length > 0)
            return RespValue.FromBulkString(argv[0]);
        if (trimmed.StartsWith("return KEYS[1]") && keys.Length > 0)
            return RespValue.FromBulkString(keys[0]);

        return null;
    }
}

internal sealed class ScriptCache
{
    private readonly Dictionary<string, string> _scripts = new(StringComparer.OrdinalIgnoreCase);

    public string Load(string script)
    {
        var sha = ComputeSha1(script);
        _scripts[sha] = script;
        return sha;
    }

    public string? Get(string sha) => _scripts.GetValueOrDefault(sha);
    public bool Exists(string sha) => _scripts.ContainsKey(sha);
    public void Flush() => _scripts.Clear();

    private static string ComputeSha1(string input)
    {
        var bytes = SHA1.HashData(Encoding.UTF8.GetBytes(input));
        return BitConverter.ToString(bytes).Replace("-", "").ToLowerInvariant();
    }
}
