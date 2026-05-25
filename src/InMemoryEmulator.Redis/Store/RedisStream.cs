namespace InMemoryEmulator.Redis.Store;

internal sealed class RedisStream : RedisEntry
{
    public List<StreamEntry> Entries { get; } = new();
    public Dictionary<string, ConsumerGroup> ConsumerGroups { get; } = new(StringComparer.Ordinal);
    public long LastTimestamp { get; set; }
    public long LastSequence { get; set; }
    public override string TypeName => "stream";

    public string GenerateId()
    {
        // Ref: https://redis.io/docs/latest/commands/xadd/
        //   "The ID is composed of <millisecondsTime>-<sequenceNumber>."
        //   "If the current time is behind LastTimestamp (e.g., after XSETID or clock skew),
        //    Redis keeps LastTimestamp and increments the sequence."
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (now > LastTimestamp)
        {
            LastTimestamp = now;
            LastSequence = 0;
        }
        else
        {
            LastSequence++;
        }
        return $"{LastTimestamp}-{LastSequence}";
    }

    public string GenerateIdAfter(string minId)
    {
        var parts = minId.Split('-');
        var ts = long.Parse(parts[0]);
        var seq = parts.Length > 1 ? long.Parse(parts[1]) : 0L;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (now > ts)
        {
            LastTimestamp = now;
            LastSequence = 0;
        }
        else if (now == ts)
        {
            LastTimestamp = ts;
            LastSequence = seq + 1;
        }
        else
        {
            LastTimestamp = ts;
            LastSequence = seq + 1;
        }
        return $"{LastTimestamp}-{LastSequence}";
    }

    public static int CompareIds(string a, string b)
    {
        var aParts = a.Split('-');
        var bParts = b.Split('-');
        var aTs = long.Parse(aParts[0]);
        var bTs = long.Parse(bParts[0]);
        if (aTs != bTs) return aTs.CompareTo(bTs);
        var aSeq = aParts.Length > 1 ? long.Parse(aParts[1]) : 0L;
        var bSeq = bParts.Length > 1 ? long.Parse(bParts[1]) : 0L;
        return aSeq.CompareTo(bSeq);
    }
}

internal sealed class StreamEntry
{
    public required string Id { get; init; }
    public required Dictionary<string, byte[]> Fields { get; init; }
}

internal sealed class ConsumerGroup
{
    public required string Name { get; init; }
    public string LastDeliveredId { get; set; } = "0-0";
    public Dictionary<string, ConsumerInfo> Consumers { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, PendingEntry> PendingEntries { get; } = new(StringComparer.Ordinal);
}

internal sealed class ConsumerInfo
{
    public required string Name { get; init; }
    public int PendingCount { get; set; }
    public DateTimeOffset LastSeen { get; set; } = DateTimeOffset.UtcNow;
}

internal sealed class PendingEntry
{
    public required string EntryId { get; init; }
    public string ConsumerName { get; set; } = "";
    public DateTimeOffset DeliveredAt { get; set; } = DateTimeOffset.UtcNow;
    public int DeliveryCount { get; set; } = 1;
}
