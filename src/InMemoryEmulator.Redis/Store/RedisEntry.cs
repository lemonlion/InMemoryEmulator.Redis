namespace InMemoryEmulator.Redis.Store;

internal abstract class RedisEntry
{
    public DateTimeOffset? Expiry { get; set; }
    public bool IsExpired => Expiry.HasValue && Expiry.Value <= DateTimeOffset.UtcNow;
    public abstract string TypeName { get; }
}
