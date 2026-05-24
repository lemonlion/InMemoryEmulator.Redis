using System.Collections.Concurrent;

namespace InMemoryEmulator.Redis.Server;

public sealed class CommandLog
{
    private readonly ConcurrentBag<CommandRecord> _records = new();

    public int Count => _records.Count;

    internal void Record(CommandRecord record) => _records.Add(record);

    public IReadOnlyList<CommandRecord> GetAll() => _records.ToArray();

    public IReadOnlyList<CommandRecord> GetByCommand(string commandName) =>
        _records.Where(r => r.CommandName.Equals(commandName, StringComparison.OrdinalIgnoreCase)).ToList();

    public void Clear()
    {
        while (_records.TryTake(out _)) { }
    }
}

public sealed record CommandRecord
{
    public required string CommandName { get; init; }
    public required string[] Arguments { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required int DatabaseIndex { get; init; }
}
