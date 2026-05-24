using System.Buffers;
using System.Text;

namespace InMemoryEmulator.Redis.Server;

internal static class RespWriter
{
    private static readonly byte[] Crlf = "\r\n"u8.ToArray();

    public static void WriteValue(IBufferWriter<byte> writer, RespValue value)
    {
        switch (value)
        {
            case RespValue.SimpleString s:
                WritePrefix(writer, '+');
                WriteRaw(writer, Encoding.UTF8.GetBytes(s.Value));
                WriteCrlf(writer);
                break;

            case RespValue.Error e:
                WritePrefix(writer, '-');
                var errMsg = string.IsNullOrEmpty(e.Message) ? e.Prefix : $"{e.Prefix} {e.Message}";
                WriteRaw(writer, Encoding.UTF8.GetBytes(errMsg));
                WriteCrlf(writer);
                break;

            case RespValue.Integer i:
                WritePrefix(writer, ':');
                WriteRaw(writer, Encoding.UTF8.GetBytes(i.Value.ToString()));
                WriteCrlf(writer);
                break;

            case RespValue.BulkString b:
                if (b.Data == null)
                {
                    WriteRaw(writer, "$-1\r\n"u8);
                }
                else
                {
                    WritePrefix(writer, '$');
                    WriteRaw(writer, Encoding.UTF8.GetBytes(b.Data.Length.ToString()));
                    WriteCrlf(writer);
                    WriteRaw(writer, b.Data);
                    WriteCrlf(writer);
                }
                break;

            case RespValue.Array a:
                if (a.Items == null)
                {
                    WriteRaw(writer, "*-1\r\n"u8);
                }
                else
                {
                    WritePrefix(writer, '*');
                    WriteRaw(writer, Encoding.UTF8.GetBytes(a.Items.Length.ToString()));
                    WriteCrlf(writer);
                    foreach (var item in a.Items)
                        WriteValue(writer, item);
                }
                break;
        }
    }

    private static void WritePrefix(IBufferWriter<byte> writer, char prefix)
    {
        var span = writer.GetSpan(1);
        span[0] = (byte)prefix;
        writer.Advance(1);
    }

    private static void WriteRaw(IBufferWriter<byte> writer, ReadOnlySpan<byte> data)
    {
        var span = writer.GetSpan(data.Length);
        data.CopyTo(span);
        writer.Advance(data.Length);
    }

    private static void WriteCrlf(IBufferWriter<byte> writer)
    {
        WriteRaw(writer, Crlf);
    }
}
