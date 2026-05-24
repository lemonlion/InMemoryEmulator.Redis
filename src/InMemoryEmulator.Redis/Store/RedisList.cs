namespace InMemoryEmulator.Redis.Store;

internal sealed class RedisList : RedisEntry
{
    public LinkedList<byte[]> Items { get; } = new();
    public override string TypeName => "list";
}
