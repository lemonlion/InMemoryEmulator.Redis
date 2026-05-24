namespace InMemoryEmulator.Redis.Server;

internal interface ILuaScriptEngine
{
    ValueTask<RespValue> EvalAsync(string script, string[] keys, string[] argv,
        Func<string, string[], ValueTask<RespValue>> callRedis);
}
