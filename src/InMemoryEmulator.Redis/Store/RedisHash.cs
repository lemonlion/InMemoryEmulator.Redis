namespace InMemoryEmulator.Redis.Store;

internal sealed class RedisHash : RedisEntry
{
    public Dictionary<string, byte[]> Fields { get; } = new(StringComparer.Ordinal);
    public override string TypeName => "hash";
}
