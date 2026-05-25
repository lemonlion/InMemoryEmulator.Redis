namespace InMemoryEmulator.Redis.Store;

internal sealed class RedisHash : RedisEntry
{
    public Dictionary<string, byte[]> Fields { get; } = new(StringComparer.Ordinal);
    public override string TypeName => "hash";

    // Ref: https://redis.io/docs/latest/commands/hexpire/
    //   Redis 7.4 introduced per-field expiration for hash data types.
    public Dictionary<string, DateTimeOffset> FieldExpiry { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Checks if a field is expired and lazily removes it if so.
    /// </summary>
    public bool IsFieldExpired(string field)
    {
        if (FieldExpiry.TryGetValue(field, out var expiry) && expiry <= DateTimeOffset.UtcNow)
        {
            Fields.Remove(field);
            FieldExpiry.Remove(field);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Gets a field value, returning null if the field does not exist or is expired.
    /// </summary>
    public byte[]? GetField(string field)
    {
        if (IsFieldExpired(field)) return null;
        return Fields.GetValueOrDefault(field);
    }

    /// <summary>
    /// Checks if a field exists and is not expired.
    /// </summary>
    public bool FieldExists(string field)
    {
        if (IsFieldExpired(field)) return false;
        return Fields.ContainsKey(field);
    }

    /// <summary>
    /// Removes all expired fields and returns the list of active field keys.
    /// </summary>
    public List<string> GetActiveFields()
    {
        // First, collect expired keys
        var expired = new List<string>();
        foreach (var kvp in FieldExpiry)
        {
            if (kvp.Value <= DateTimeOffset.UtcNow)
                expired.Add(kvp.Key);
        }
        // Remove expired fields
        foreach (var key in expired)
        {
            Fields.Remove(key);
            FieldExpiry.Remove(key);
        }
        return Fields.Keys.ToList();
    }

    /// <summary>
    /// Gets the count of non-expired fields.
    /// </summary>
    public int ActiveFieldCount
    {
        get
        {
            GetActiveFields(); // purge expired
            return Fields.Count;
        }
    }
}
