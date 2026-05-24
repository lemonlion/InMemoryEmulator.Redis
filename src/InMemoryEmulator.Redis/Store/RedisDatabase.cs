using System.Collections.Concurrent;

namespace InMemoryEmulator.Redis.Store;

internal sealed class RedisDatabase : IDisposable
{
    private readonly ConcurrentDictionary<string, RedisEntry> _entries = new();
    private readonly ConcurrentDictionary<string, long> _keyVersions = new();
    private readonly ExpiryManager _expiryManager;
    private long _versionCounter;

    public RedisDatabase()
    {
        _expiryManager = new ExpiryManager(this);
    }

    public void Dispose() => _expiryManager.Dispose();

    public RedisEntry? GetEntry(string key)
    {
        if (!_entries.TryGetValue(key, out var entry)) return null;
        if (entry.IsExpired)
        {
            _entries.TryRemove(key, out _);
            return null;
        }
        return entry;
    }

    public T? GetTyped<T>(string key) where T : RedisEntry
    {
        var entry = GetEntry(key);
        if (entry == null) return null;
        if (entry is not T typed)
            throw new WrongTypeException();
        return typed;
    }

    public void SetEntry(string key, RedisEntry entry)
    {
        _entries[key] = entry;
        IncrementVersion(key);
    }

    public bool RemoveEntry(string key)
    {
        var removed = _entries.TryRemove(key, out _);
        if (removed) IncrementVersion(key);
        return removed;
    }

    public bool KeyExists(string key) => GetEntry(key) != null;

    public string GetKeyType(string key)
    {
        var entry = GetEntry(key);
        return entry?.TypeName ?? "none";
    }

    public long GetKeyVersion(string key)
    {
        _keyVersions.TryGetValue(key, out var version);
        return version;
    }

    internal void IncrementVersion(string key)
    {
        var newVersion = Interlocked.Increment(ref _versionCounter);
        _keyVersions[key] = newVersion;
    }

    public int Count => _entries.Count(kvp => !kvp.Value.IsExpired);

    public void FlushDb()
    {
        _entries.Clear();
        _keyVersions.Clear();
    }

    internal ConcurrentDictionary<string, RedisEntry> RawEntries => _entries;
}

internal sealed class WrongTypeException : Exception
{
    public WrongTypeException() : base("WRONGTYPE Operation against a key holding the wrong kind of value") { }
}
