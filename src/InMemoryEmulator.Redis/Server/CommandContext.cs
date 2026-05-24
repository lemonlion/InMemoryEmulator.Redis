using System.Text;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Server;

internal sealed class CommandContext
{
    public required string CommandName { get; init; }
    public required RespValue[] Arguments { get; init; }
    public required ClientConnection Client { get; init; }
    public required RedisDatabase Database { get; init; }
    public CancellationToken CancellationToken { get; init; }

    public string GetArgString(int index)
    {
        if (index >= Arguments.Length)
            throw new ArgumentOutOfRangeException(nameof(index));
        return Arguments[index] switch
        {
            RespValue.BulkString { Data: { } data } => Encoding.UTF8.GetString(data),
            RespValue.SimpleString { Value: var v } => v,
            RespValue.Integer { Value: var v } => v.ToString(),
            _ => ""
        };
    }

    public byte[]? GetArgBytes(int index)
    {
        if (index >= Arguments.Length)
            throw new ArgumentOutOfRangeException(nameof(index));
        return Arguments[index] switch
        {
            RespValue.BulkString { Data: var data } => data,
            RespValue.SimpleString { Value: var v } => Encoding.UTF8.GetBytes(v),
            _ => null
        };
    }

    public long GetArgLong(int index)
    {
        var str = GetArgString(index);
        return long.Parse(str);
    }

    public double GetArgDouble(int index)
    {
        var str = GetArgString(index);
        return double.Parse(str);
    }
}
