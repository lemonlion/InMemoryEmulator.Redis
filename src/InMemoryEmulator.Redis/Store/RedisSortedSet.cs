namespace InMemoryEmulator.Redis.Store;

internal sealed class RedisSortedSet : RedisEntry
{
    public Dictionary<string, double> MemberScores { get; } = new(StringComparer.Ordinal);

    public SortedSet<(double Score, string Member)> ScoreIndex { get; } =
        new(Comparer<(double Score, string Member)>.Create((a, b) =>
        {
            int cmp = a.Score.CompareTo(b.Score);
            return cmp != 0 ? cmp : string.CompareOrdinal(a.Member, b.Member);
        }));

    public override string TypeName => "zset";

    public bool Add(string member, double score)
    {
        if (MemberScores.TryGetValue(member, out var existing))
        {
            ScoreIndex.Remove((existing, member));
            MemberScores[member] = score;
            ScoreIndex.Add((score, member));
            return false;
        }
        MemberScores[member] = score;
        ScoreIndex.Add((score, member));
        return true;
    }

    public bool Remove(string member)
    {
        if (!MemberScores.TryGetValue(member, out var score)) return false;
        MemberScores.Remove(member);
        ScoreIndex.Remove((score, member));
        return true;
    }

    public double? GetScore(string member)
    {
        return MemberScores.TryGetValue(member, out var score) ? score : null;
    }

    public long? GetRank(string member)
    {
        if (!MemberScores.TryGetValue(member, out var score)) return null;
        int rank = 0;
        foreach (var item in ScoreIndex)
        {
            if (item.Member == member) return rank;
            rank++;
        }
        return null;
    }

    public long? GetRevRank(string member)
    {
        var rank = GetRank(member);
        if (rank == null) return null;
        return ScoreIndex.Count - 1 - rank;
    }
}
