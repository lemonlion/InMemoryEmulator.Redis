namespace InMemoryEmulator.Redis.Store;

internal sealed class RedisString : RedisEntry
{
    public byte[] Value { get; set; } = Array.Empty<byte>();
    public override string TypeName => "string";
}
