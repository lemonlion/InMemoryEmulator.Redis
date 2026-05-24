namespace InMemoryEmulator.Redis.Store;

// Ref: https://redis.io/docs/latest/commands/expire/
//   "Keys are lazily expired on access. Redis also does active expiration:
//    it tests 20 random keys per cycle (100ms default interval)."
internal sealed class ExpiryManager : IDisposable
{
    private readonly Timer _timer;
    private readonly RedisDatabase _database;
    private static readonly Random _random = new();

    public ExpiryManager(RedisDatabase database)
    {
        _database = database;
        _timer = new Timer(ActiveExpiryCycle, null,
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(100));
    }

    private void ActiveExpiryCycle(object? state)
    {
        var entries = _database.RawEntries;
        var keys = entries.Keys.ToArray();
        if (keys.Length == 0) return;

        var sampleSize = Math.Min(20, keys.Length);
        for (int i = 0; i < sampleSize; i++)
        {
            var key = keys[_random.Next(keys.Length)];
            if (entries.TryGetValue(key, out var entry) && entry.IsExpired)
                entries.TryRemove(key, out _);
        }
    }

    public void Dispose() => _timer.Dispose();
}
