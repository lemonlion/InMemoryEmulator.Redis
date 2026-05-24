namespace InMemoryEmulator.Redis.LuaScripting;

public static class LuaScriptingExtensions
{
    /// <summary>
    /// Enables full Lua scripting support via the MoonSharp interpreter.
    /// Call this after creating the InMemoryRedisResult.
    /// </summary>
    public static InMemoryRedisResult UseLuaScripting(this InMemoryRedisResult result)
    {
        result.Server.SetLuaEngine(new MoonSharpLuaEngine());
        return result;
    }
}
