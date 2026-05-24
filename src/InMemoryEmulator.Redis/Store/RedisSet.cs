namespace InMemoryEmulator.Redis.Store;

internal sealed class RedisSet : RedisEntry
{
    public HashSet<string> Members { get; } = new(StringComparer.Ordinal);
    public override string TypeName => "set";
}
