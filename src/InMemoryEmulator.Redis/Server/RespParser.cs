using System.Buffers;
using System.IO.Pipelines;
using System.Text;

namespace InMemoryEmulator.Redis.Server;

internal static class RespParser
{
    private static readonly byte[] Crlf = "\r\n"u8.ToArray();

    public static async ValueTask<RespValue?> ReadValueAsync(PipeReader reader, CancellationToken ct)
    {
        while (true)
        {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (TryParseValue(ref buffer, out var value))
            {
                reader.AdvanceTo(buffer.Start);
                return value;
            }

            if (result.IsCompleted)
            {
                reader.AdvanceTo(buffer.Start);
                return null;
            }

            reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    private static bool TryParseValue(ref ReadOnlySequence<byte> buffer, out RespValue? value)
    {
        value = null;
        if (buffer.IsEmpty) return false;

        var reader = new SequenceReader<byte>(buffer);
        if (!reader.TryPeek(out var prefix)) return false;

        bool success = prefix switch
        {
            (byte)'+' => TryParseSimpleString(ref reader, out value),
            (byte)'-' => TryParseError(ref reader, out value),
            (byte)':' => TryParseInteger(ref reader, out value),
            (byte)'$' => TryParseBulkString(ref reader, out value),
            (byte)'*' => TryParseArray(ref reader, out value),
            _ => TryParseInlineCommand(ref reader, out value)
        };

        if (success)
            buffer = buffer.Slice(reader.Position);

        return success;
    }

    private static bool TryParseSimpleString(ref SequenceReader<byte> reader, out RespValue? value)
    {
        value = null;
        reader.Advance(1); // skip '+'
        if (!TryReadLine(ref reader, out var line)) return false;
        value = new RespValue.SimpleString(Encoding.UTF8.GetString(line));
        return true;
    }

    private static bool TryParseError(ref SequenceReader<byte> reader, out RespValue? value)
    {
        value = null;
        reader.Advance(1); // skip '-'
        if (!TryReadLine(ref reader, out var line)) return false;
        var msg = Encoding.UTF8.GetString(line);
        var spaceIdx = msg.IndexOf(' ');
        if (spaceIdx > 0)
            value = new RespValue.Error(msg[..spaceIdx], msg[(spaceIdx + 1)..]);
        else
            value = new RespValue.Error(msg, "");
        return true;
    }

    private static bool TryParseInteger(ref SequenceReader<byte> reader, out RespValue? value)
    {
        value = null;
        reader.Advance(1); // skip ':'
        if (!TryReadLine(ref reader, out var line)) return false;
        value = new RespValue.Integer(long.Parse(Encoding.UTF8.GetString(line)));
        return true;
    }

    private static bool TryParseBulkString(ref SequenceReader<byte> reader, out RespValue? value)
    {
        value = null;
        reader.Advance(1); // skip '$'
        if (!TryReadLine(ref reader, out var lenLine)) return false;

        var length = int.Parse(Encoding.UTF8.GetString(lenLine));
        if (length == -1)
        {
            value = RespValue.NullBulkString;
            return true;
        }

        if (reader.Remaining < length + 2) return false; // need data + \r\n

        var data = new byte[length];
        reader.TryCopyTo(data.AsSpan());
        reader.Advance(length);

        // consume trailing \r\n
        if (reader.Remaining < 2) return false;
        reader.Advance(2);

        value = new RespValue.BulkString(data);
        return true;
    }

    private static bool TryParseArray(ref SequenceReader<byte> reader, out RespValue? value)
    {
        value = null;
        var startPosition = reader.Position;
        reader.Advance(1); // skip '*'
        if (!TryReadLine(ref reader, out var countLine))
        {
            reader.Rewind(reader.Consumed);
            return false;
        }

        var count = int.Parse(Encoding.UTF8.GetString(countLine));
        if (count == -1)
        {
            value = RespValue.NullArray;
            return true;
        }

        var items = new RespValue[count];
        for (int i = 0; i < count; i++)
        {
            if (!TryPeekAndParse(ref reader, out var item))
                return false;
            items[i] = item!;
        }

        value = new RespValue.Array(items);
        return true;
    }

    private static bool TryPeekAndParse(ref SequenceReader<byte> reader, out RespValue? value)
    {
        value = null;
        if (!reader.TryPeek(out var prefix)) return false;

        return prefix switch
        {
            (byte)'+' => TryParseSimpleString(ref reader, out value),
            (byte)'-' => TryParseError(ref reader, out value),
            (byte)':' => TryParseInteger(ref reader, out value),
            (byte)'$' => TryParseBulkString(ref reader, out value),
            (byte)'*' => TryParseArray(ref reader, out value),
            _ => false
        };
    }

    private static bool TryParseInlineCommand(ref SequenceReader<byte> reader, out RespValue? value)
    {
        value = null;
        if (!TryReadLine(ref reader, out var line)) return false;
        var text = Encoding.UTF8.GetString(line).Trim();
        if (string.IsNullOrEmpty(text)) return false;

        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var items = parts.Select(p => (RespValue)new RespValue.BulkString(Encoding.UTF8.GetBytes(p))).ToArray();
        value = new RespValue.Array(items);
        return true;
    }

    private static bool TryReadLine(ref SequenceReader<byte> reader, out ReadOnlySpan<byte> line)
    {
        line = default;
        if (!reader.TryReadTo(out ReadOnlySequence<byte> seq, Crlf.AsSpan(), advancePastDelimiter: true))
            return false;
        line = seq.IsSingleSegment ? seq.FirstSpan : seq.ToArray().AsSpan();
        return true;
    }
}
