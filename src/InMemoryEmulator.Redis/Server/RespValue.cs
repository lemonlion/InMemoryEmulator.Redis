using System.Text;

namespace InMemoryEmulator.Redis.Server;

internal abstract record RespValue
{
    internal sealed record SimpleString(string Value) : RespValue;
    internal sealed record Error(string Prefix, string Message) : RespValue;
    internal sealed record Integer(long Value) : RespValue;
    internal sealed record BulkString(byte[]? Data) : RespValue;
    internal sealed record Array(RespValue[]? Items) : RespValue;

    internal static readonly RespValue Ok = new SimpleString("OK");
    internal static readonly RespValue Pong = new SimpleString("PONG");
    internal static readonly RespValue Queued = new SimpleString("QUEUED");
    internal static readonly RespValue NullBulkString = new BulkString(null);
    internal static readonly RespValue NullArray = new Array(null);
    internal static readonly RespValue EmptyArray = new Array(System.Array.Empty<RespValue>());
    internal static readonly RespValue Zero = new Integer(0);
    internal static readonly RespValue One = new Integer(1);
    internal static readonly RespValue NegativeOne = new Integer(-1);
    internal static readonly RespValue NegativeTwo = new Integer(-2);

    internal static RespValue FromBulkString(string value) =>
        new BulkString(Encoding.UTF8.GetBytes(value));

    internal static RespValue WrongTypeError =>
        new Error("WRONGTYPE", "Operation against a key holding the wrong kind of value");

    // Sentinel value: command handler already wrote the response directly
    internal static readonly RespValue NoResponse = new SimpleString("__NO_RESPONSE__");
}
