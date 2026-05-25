using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class BitmapCommands : ICommandHandler
{
    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "SETBIT" => SetBit(context),
            "GETBIT" => GetBit(context),
            "BITCOUNT" => BitCount(context),
            "BITOP" => BitOp(context),
            "BITPOS" => BitPos(context),
            "BITFIELD" => BitField(context),
            "BITFIELD_RO" => BitFieldRo(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    // Ref: https://redis.io/docs/latest/commands/setbit/
    //   "Sets or clears the bit at offset in the string value stored at key."
    //   "When key does not exist, a new string value is created."
    //   "The string is grown to make sure it can hold a bit at offset."
    //   "The bit value that was previously stored at that offset is returned."
    //   "Bit offsets start at bit 0 being the most significant bit of the first byte."
    private static ValueTask<RespValue> SetBit(CommandContext ctx)
    {
        if (ctx.Arguments.Length < 3)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'setbit' command"));

        var key = ctx.GetArgString(0);
        var offset = ctx.GetArgLong(1);
        var bitValue = ctx.GetArgLong(2);

        // Ref: https://redis.io/docs/latest/commands/setbit/
        //   "The offset argument is required to be greater than or equal to 0"
        if (offset < 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "bit offset is not an integer or out of range"));

        // Ref: https://redis.io/docs/latest/commands/setbit/
        //   "The bit is either set or cleared depending on value, which can be either 0 or 1."
        if (bitValue != 0 && bitValue != 1)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "bit is not an integer or out of range"));

        var entry = ctx.Database.GetTyped<RedisString>(key);
        byte[] data;
        if (entry != null)
        {
            data = entry.Value;
        }
        else
        {
            data = Array.Empty<byte>();
        }

        int byteIndex = (int)(offset / 8);
        int bitIndex = (int)(7 - (offset % 8)); // MSB first within each byte

        // Auto-extend with zero bytes if needed
        if (byteIndex >= data.Length)
        {
            var newData = new byte[byteIndex + 1];
            data.CopyTo(newData, 0);
            data = newData;
        }

        // Get old bit value
        int oldBit = (data[byteIndex] >> bitIndex) & 1;

        // Set new bit value
        if (bitValue == 1)
            data[byteIndex] = (byte)(data[byteIndex] | (1 << bitIndex));
        else
            data[byteIndex] = (byte)(data[byteIndex] & ~(1 << bitIndex));

        if (entry != null)
        {
            entry.Value = data;
            ctx.Database.IncrementVersion(key);
        }
        else
        {
            ctx.Database.SetEntry(key, new RedisString { Value = data });
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Integer(oldBit));
    }

    // Ref: https://redis.io/docs/latest/commands/getbit/
    //   "Returns the bit value at offset in the string value stored at key."
    //   "When offset is beyond the string length, the string is assumed to be a
    //    contiguous space with 0 bits."
    //   "When key does not exist it is assumed to be an empty string, so offset
    //    is always out of range and the value is also assumed to be a contiguous
    //    space with 0 bits."
    private static ValueTask<RespValue> GetBit(CommandContext ctx)
    {
        if (ctx.Arguments.Length < 2)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'getbit' command"));

        var key = ctx.GetArgString(0);
        var offset = ctx.GetArgLong(1);

        if (offset < 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "bit offset is not an integer or out of range"));

        var entry = ctx.Database.GetTyped<RedisString>(key);
        if (entry == null)
            return ValueTask.FromResult(RespValue.Zero);

        int byteIndex = (int)(offset / 8);
        int bitIndex = (int)(7 - (offset % 8));

        if (byteIndex >= entry.Value.Length)
            return ValueTask.FromResult(RespValue.Zero);

        int bit = (entry.Value[byteIndex] >> bitIndex) & 1;
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(bit));
    }

    // Ref: https://redis.io/docs/latest/commands/bitcount/
    //   "Count the number of set bits (population counting) in a string."
    //   "By default all the bytes contained in the string are examined."
    //   "It is possible to specify the counting operation only in an interval
    //    passing the additional arguments start and end."
    //   "Like for the GETRANGE command start and end can contain negative values
    //    in order to index bytes starting from the end of the string."
    //   "Non-existent keys are treated as empty strings, so the command will
    //    return zero."
    //   "By default, the additional arguments start and end specify a byte range.
    //    Starting with Redis 7.0, you can use the optional BIT modifier to specify
    //    a bit range."
    private static ValueTask<RespValue> BitCount(CommandContext ctx)
    {
        if (ctx.Arguments.Length < 1)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'bitcount' command"));

        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetTyped<RedisString>(key);
        if (entry == null)
            return ValueTask.FromResult(RespValue.Zero);

        var data = entry.Value;
        if (data.Length == 0)
            return ValueTask.FromResult(RespValue.Zero);

        if (ctx.Arguments.Length >= 3)
        {
            var start = (int)ctx.GetArgLong(1);
            var end = (int)ctx.GetArgLong(2);

            // Check for BIT mode (Redis 7.0+)
            bool bitMode = false;
            if (ctx.Arguments.Length >= 4)
            {
                var mode = ctx.GetArgString(3).ToUpperInvariant();
                if (mode == "BIT")
                    bitMode = true;
                // BYTE is the default
            }

            if (bitMode)
            {
                // BIT mode: start and end are bit indices
                int totalBits = data.Length * 8;

                // Normalize negative indices
                if (start < 0) start = totalBits + start;
                if (end < 0) end = totalBits + end;

                // Clamp
                if (start < 0) start = 0;
                if (end >= totalBits) end = totalBits - 1;

                if (start > end)
                    return ValueTask.FromResult(RespValue.Zero);

                long count = 0;
                for (int i = start; i <= end; i++)
                {
                    int byteIdx = i / 8;
                    int bitIdx = 7 - (i % 8);
                    if ((data[byteIdx] >> bitIdx & 1) == 1)
                        count++;
                }
                return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
            }
            else
            {
                // BYTE mode (default): start and end are byte indices
                int len = data.Length;

                // Normalize negative indices
                if (start < 0) start = len + start;
                if (end < 0) end = len + end;

                // Clamp
                if (start < 0) start = 0;
                if (end >= len) end = len - 1;

                if (start > end)
                    return ValueTask.FromResult(RespValue.Zero);

                long count = 0;
                for (int i = start; i <= end; i++)
                    count += PopCount(data[i]);

                return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
            }
        }

        // No range: count all bits in entire string
        long total = 0;
        for (int i = 0; i < data.Length; i++)
            total += PopCount(data[i]);

        return ValueTask.FromResult<RespValue>(new RespValue.Integer(total));
    }

    // Ref: https://redis.io/docs/latest/commands/bitop/
    //   "Perform a bitwise operation between multiple keys (containing string values)
    //    and store the result in the destination key."
    //   "The BITOP command supports four bitwise operations: AND, OR, XOR and NOT."
    //   "NOT is special as it only takes an input key."
    //   "The result of the operation is always stored at destkey."
    //   "The size of the string stored in the destination key is equal to the size of
    //    the longest input string."
    //   "When an operation is performed between strings having different lengths,
    //    all the strings shorter than the longest string in the set are treated as
    //    if they were zero-padded up to the length of the longest string."
    //   "The same holds true for non-existent keys that are regarded as a sequence
    //    of zero bytes up to the length of the longest string."
    //   "Return value: Integer reply — the size of the string stored in the destination key"
    private static ValueTask<RespValue> BitOp(CommandContext ctx)
    {
        if (ctx.Arguments.Length < 3)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'bitop' command"));

        var operation = ctx.GetArgString(0).ToUpperInvariant();
        var destKey = ctx.GetArgString(1);

        if (operation == "NOT")
        {
            // NOT takes exactly one source key
            if (ctx.Arguments.Length != 3)
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "BITOP NOT requires one and only one key."));

            var srcKey = ctx.GetArgString(2);
            var srcEntry = ctx.Database.GetTyped<RedisString>(srcKey);
            var srcData = srcEntry?.Value ?? Array.Empty<byte>();

            var result = new byte[srcData.Length];
            for (int i = 0; i < srcData.Length; i++)
                result[i] = (byte)~srcData[i];

            ctx.Database.SetEntry(destKey, new RedisString { Value = result });
            return ValueTask.FromResult<RespValue>(new RespValue.Integer(result.Length));
        }

        if (operation != "AND" && operation != "OR" && operation != "XOR")
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "BITOP requires AND, OR, XOR, or NOT operation."));

        // Gather all source keys
        var sources = new List<byte[]>();
        int maxLen = 0;
        for (int i = 2; i < ctx.Arguments.Length; i++)
        {
            var srcKey = ctx.GetArgString(i);
            var srcEntry = ctx.Database.GetTyped<RedisString>(srcKey);
            var srcData = srcEntry?.Value ?? Array.Empty<byte>();
            sources.Add(srcData);
            if (srcData.Length > maxLen) maxLen = srcData.Length;
        }

        var resultData = new byte[maxLen];

        switch (operation)
        {
            case "AND":
                // Initialize to 0xFF then AND with each source
                for (int i = 0; i < maxLen; i++)
                    resultData[i] = 0xFF;
                foreach (var src in sources)
                {
                    for (int i = 0; i < maxLen; i++)
                    {
                        byte srcByte = i < src.Length ? src[i] : (byte)0;
                        resultData[i] &= srcByte;
                    }
                }
                break;

            case "OR":
                foreach (var src in sources)
                {
                    for (int i = 0; i < src.Length; i++)
                        resultData[i] |= src[i];
                }
                break;

            case "XOR":
                foreach (var src in sources)
                {
                    for (int i = 0; i < src.Length; i++)
                        resultData[i] ^= src[i];
                }
                break;
        }

        ctx.Database.SetEntry(destKey, new RedisString { Value = resultData });
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(resultData.Length));
    }

    // Ref: https://redis.io/docs/latest/commands/bitpos/
    //   "Return the position of the first bit set to 1 or 0 in a string."
    //   "By default, all the bytes contained in the string are examined."
    //   "The position is returned, thinking of the string as an array of bits from
    //    left to right, where the first byte's most significant bit is at position 0."
    //   "An empty string is treated as a special case: the command always returns -1."
    //   "By default, the range is interpreted as a range of bytes and not a range of bits,
    //    so start=0 and end=2 means to look at the first three bytes."
    //   "Since Redis 7.0, we can use the optional BIT modifier to specify that the range
    //    should be interpreted as a range of bits."
    //   "When bit=0: If not found within range, returns first 0 bit position right after
    //    last byte in range (if end is not given or range covers entire string).
    //    If range is explicit and no 0 bit found, returns -1."
    //   "When bit=1: If not found, returns -1."
    private static ValueTask<RespValue> BitPos(CommandContext ctx)
    {
        if (ctx.Arguments.Length < 2)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'bitpos' command"));

        var key = ctx.GetArgString(0);
        var targetBit = (int)ctx.GetArgLong(1);

        if (targetBit != 0 && targetBit != 1)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "bit is not an integer or out of range"));

        var entry = ctx.Database.GetTyped<RedisString>(key);

        // Non-existent key is treated as empty string
        if (entry == null || entry.Value.Length == 0)
        {
            // Ref: https://redis.io/docs/latest/commands/bitpos/
            //   "If we look for set bits and the string is empty or set to zero, -1 is returned."
            //   "If we look for clear bits and the string only contains bits set to 1,
            //    the function returns the first bit beyond the last byte of the string.
            //    However, if the user specifies a range, and no 0 bit is found, -1 is returned."
            if (targetBit == 1)
                return ValueTask.FromResult(RespValue.NegativeOne);
            // For bit=0 and empty string, return 0 (first position beyond string)
            // unless we have an explicit range
            if (ctx.Arguments.Length >= 3)
                return ValueTask.FromResult(RespValue.NegativeOne);
            return ValueTask.FromResult(RespValue.Zero);
        }

        var data = entry.Value;
        bool hasStart = ctx.Arguments.Length >= 3;
        bool hasEnd = ctx.Arguments.Length >= 4;
        bool bitMode = false;
        bool endExplicit = false;

        int start = 0;
        int end;

        if (ctx.Arguments.Length >= 5)
        {
            var mode = ctx.GetArgString(4).ToUpperInvariant();
            if (mode == "BIT")
                bitMode = true;
        }
        // Also check position 3 for mode when there are 4 args
        // (start + BIT/BYTE without end — but Redis requires end before BIT/BYTE modifier)

        if (bitMode)
        {
            // BIT mode
            int totalBits = data.Length * 8;
            start = hasStart ? (int)ctx.GetArgLong(2) : 0;
            end = hasEnd ? (int)ctx.GetArgLong(3) : totalBits - 1;

            if (start < 0) start = totalBits + start;
            if (end < 0) end = totalBits + end;

            if (start < 0) start = 0;
            if (end >= totalBits) end = totalBits - 1;

            if (start > end)
                return ValueTask.FromResult(RespValue.NegativeOne);

            for (int i = start; i <= end; i++)
            {
                int byteIdx = i / 8;
                int bitIdx = 7 - (i % 8);
                int bitVal = (data[byteIdx] >> bitIdx) & 1;
                if (bitVal == targetBit)
                    return ValueTask.FromResult<RespValue>(new RespValue.Integer(i));
            }

            return ValueTask.FromResult(RespValue.NegativeOne);
        }
        else
        {
            // BYTE mode (default)
            int len = data.Length;
            start = hasStart ? (int)ctx.GetArgLong(2) : 0;
            end = hasEnd ? (int)ctx.GetArgLong(3) : len - 1;
            endExplicit = hasEnd;

            if (start < 0) start = len + start;
            if (end < 0) end = len + end;

            if (start < 0) start = 0;
            if (end >= len) end = len - 1;

            if (start > end)
                return ValueTask.FromResult(RespValue.NegativeOne);

            for (int byteIdx = start; byteIdx <= end; byteIdx++)
            {
                for (int bitIdx = 7; bitIdx >= 0; bitIdx--)
                {
                    int bitVal = (data[byteIdx] >> bitIdx) & 1;
                    if (bitVal == targetBit)
                    {
                        long pos = byteIdx * 8 + (7 - bitIdx);
                        return ValueTask.FromResult<RespValue>(new RespValue.Integer(pos));
                    }
                }
            }

            // Not found in range
            if (targetBit == 0 && !endExplicit)
            {
                // Return the first bit position right after the last byte examined
                return ValueTask.FromResult<RespValue>(new RespValue.Integer((end + 1) * 8));
            }

            return ValueTask.FromResult(RespValue.NegativeOne);
        }
    }

    // Ref: https://redis.io/docs/latest/commands/bitfield/
    //   "The command treats a Redis string as an array of bits, and is capable
    //    of addressing specific integer fields of varying bit widths and arbitrary
    //    non (necessary) aligned offset."
    //   "BITFIELD supports the following sub-commands:"
    //   "GET <encoding> <offset> — Returns the specified bit field."
    //   "SET <encoding> <offset> <value> — Set the specified bit field and returns its old value."
    //   "INCRBY <encoding> <offset> <increment> — Increments or decrements the specified bit field
    //    and returns the new value."
    //   "OVERFLOW [WRAP|SAT|FAIL] — Sets the overflow behavior for subsequent SET/INCRBY operations."
    //   "Encodings: i<bits> for signed, u<bits> for unsigned. Max 64 bits for signed, 63 for unsigned."
    //   "Offsets can be prefixed with # for automatic alignment: #0 means 0, #1 means 1*bits, etc."
    private static ValueTask<RespValue> BitField(CommandContext ctx)
    {
        return BitFieldImpl(ctx, readOnly: false);
    }

    // Ref: https://redis.io/docs/latest/commands/bitfield_ro/
    //   "Read-only variant of the BITFIELD command. It is like the original BITFIELD
    //    but only accepts GET subcommand and can safely be used in read-only replicas."
    private static ValueTask<RespValue> BitFieldRo(CommandContext ctx)
    {
        return BitFieldImpl(ctx, readOnly: true);
    }

    private static ValueTask<RespValue> BitFieldImpl(CommandContext ctx, bool readOnly)
    {
        if (ctx.Arguments.Length < 1)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                $"wrong number of arguments for '{(readOnly ? "bitfield_ro" : "bitfield")}' command"));

        var key = ctx.GetArgString(0);
        var results = new List<RespValue>();
        var overflowMode = OverflowMode.Wrap; // Default overflow mode

        int i = 1;
        while (i < ctx.Arguments.Length)
        {
            var subCommand = ctx.GetArgString(i).ToUpperInvariant();
            i++;

            switch (subCommand)
            {
                case "GET":
                {
                    if (i + 1 >= ctx.Arguments.Length)
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            "wrong number of arguments for 'bitfield' command"));

                    var encoding = ctx.GetArgString(i);
                    var offsetStr = ctx.GetArgString(i + 1);
                    i += 2;

                    if (!TryParseEncoding(encoding, out bool signed, out int bits))
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            $"Invalid bitfield type. Use something like i8 u8 u4 u16 i16 i32."));

                    if (!TryParseOffset(offsetStr, bits, out long bitOffset))
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            "bit offset is not an integer or out of range"));

                    var entry = ctx.Database.GetTyped<RedisString>(key);
                    var data = entry?.Value ?? Array.Empty<byte>();

                    long value = ReadBitField(data, bitOffset, bits, signed);
                    results.Add(new RespValue.Integer(value));
                    break;
                }

                case "SET":
                {
                    if (readOnly)
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            "BITFIELD_RO only supports the GET subcommand"));

                    if (i + 2 >= ctx.Arguments.Length)
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            "wrong number of arguments for 'bitfield' command"));

                    var encoding = ctx.GetArgString(i);
                    var offsetStr = ctx.GetArgString(i + 1);
                    var setValue = ctx.GetArgLong(i + 2);
                    i += 3;

                    if (!TryParseEncoding(encoding, out bool signed, out int bits))
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            $"Invalid bitfield type. Use something like i8 u8 u4 u16 i16 i32."));

                    if (!TryParseOffset(offsetStr, bits, out long bitOffset))
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            "bit offset is not an integer or out of range"));

                    var entry = ctx.Database.GetTyped<RedisString>(key);
                    var data = entry?.Value ?? Array.Empty<byte>();

                    // Ensure data is large enough
                    data = EnsureBitFieldSize(data, bitOffset, bits);

                    long oldValue = ReadBitField(data, bitOffset, bits, signed);

                    // Wrap the value to the encoding range before writing
                    long wrappedValue = WrapValue(setValue, bits, signed);
                    WriteBitField(data, bitOffset, bits, wrappedValue);

                    if (entry != null)
                    {
                        entry.Value = data;
                        ctx.Database.IncrementVersion(key);
                    }
                    else
                    {
                        ctx.Database.SetEntry(key, new RedisString { Value = data });
                    }

                    results.Add(new RespValue.Integer(oldValue));
                    break;
                }

                case "INCRBY":
                {
                    if (readOnly)
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            "BITFIELD_RO only supports the GET subcommand"));

                    if (i + 2 >= ctx.Arguments.Length)
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            "wrong number of arguments for 'bitfield' command"));

                    var encoding = ctx.GetArgString(i);
                    var offsetStr = ctx.GetArgString(i + 1);
                    var increment = ctx.GetArgLong(i + 2);
                    i += 3;

                    if (!TryParseEncoding(encoding, out bool signed, out int bits))
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            $"Invalid bitfield type. Use something like i8 u8 u4 u16 i16 i32."));

                    if (!TryParseOffset(offsetStr, bits, out long bitOffset))
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            "bit offset is not an integer or out of range"));

                    var entry = ctx.Database.GetTyped<RedisString>(key);
                    var data = entry?.Value ?? Array.Empty<byte>();

                    data = EnsureBitFieldSize(data, bitOffset, bits);

                    long currentValue = ReadBitField(data, bitOffset, bits, signed);
                    long newValue = currentValue + increment;

                    // Apply overflow handling
                    long? resultValue = ApplyOverflow(newValue, bits, signed, overflowMode);

                    if (resultValue == null)
                    {
                        // FAIL mode: don't change value, return nil
                        results.Add(RespValue.NullBulkString);
                    }
                    else
                    {
                        WriteBitField(data, bitOffset, bits, resultValue.Value);

                        if (entry != null)
                        {
                            entry.Value = data;
                            ctx.Database.IncrementVersion(key);
                        }
                        else
                        {
                            ctx.Database.SetEntry(key, new RedisString { Value = data });
                        }

                        results.Add(new RespValue.Integer(resultValue.Value));
                    }
                    break;
                }

                case "OVERFLOW":
                {
                    if (readOnly)
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            "BITFIELD_RO only supports the GET subcommand"));

                    if (i >= ctx.Arguments.Length)
                        return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                            "wrong number of arguments for 'bitfield' command"));

                    var mode = ctx.GetArgString(i).ToUpperInvariant();
                    i++;

                    overflowMode = mode switch
                    {
                        "WRAP" => OverflowMode.Wrap,
                        "SAT" => OverflowMode.Sat,
                        "FAIL" => OverflowMode.Fail,
                        _ => throw new InvalidOperationException($"Invalid OVERFLOW type '{mode}'")
                    };
                    // OVERFLOW does not produce a result value
                    break;
                }

                default:
                    return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR",
                        $"Unknown BITFIELD subcommand '{subCommand}'"));
            }
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    #region Bitmap Helpers

    private static int PopCount(byte b)
    {
        // Count set bits in a byte
        int count = 0;
        while (b != 0)
        {
            count += b & 1;
            b >>= 1;
        }
        return count;
    }

    private enum OverflowMode
    {
        Wrap,
        Sat,
        Fail
    }

    // Ref: https://redis.io/docs/latest/commands/bitfield/
    //   "Encodings: Signed with i prefix (e.g., i8), unsigned with u prefix (e.g., u8)."
    //   "Supported encodings are up to 64 bits for signed integers and up to 63 bits
    //    for unsigned integers."
    private static bool TryParseEncoding(string encoding, out bool signed, out int bits)
    {
        signed = false;
        bits = 0;

        if (string.IsNullOrEmpty(encoding))
            return false;

        if (encoding[0] == 'i')
            signed = true;
        else if (encoding[0] == 'u')
            signed = false;
        else
            return false;

        if (!int.TryParse(encoding.Substring(1), out bits))
            return false;

        if (bits <= 0)
            return false;

        // Signed supports up to 64 bits, unsigned up to 63 bits
        if (signed && bits > 64)
            return false;
        if (!signed && bits > 63)
            return false;

        return true;
    }

    // Ref: https://redis.io/docs/latest/commands/bitfield/
    //   "Offsets in bitfield can optionally be prefixed with # to mean that the
    //    specified offset is multiplied by the integer encoding's width."
    private static bool TryParseOffset(string offsetStr, int bits, out long bitOffset)
    {
        bitOffset = 0;
        if (string.IsNullOrEmpty(offsetStr))
            return false;

        if (offsetStr[0] == '#')
        {
            if (!long.TryParse(offsetStr.Substring(1), out long multiplier))
                return false;
            bitOffset = multiplier * bits;
            return true;
        }

        return long.TryParse(offsetStr, out bitOffset) && bitOffset >= 0;
    }

    private static byte[] EnsureBitFieldSize(byte[] data, long bitOffset, int bits)
    {
        long lastBit = bitOffset + bits - 1;
        int requiredBytes = (int)(lastBit / 8) + 1;
        if (requiredBytes > data.Length)
        {
            var newData = new byte[requiredBytes];
            data.CopyTo(newData, 0);
            return newData;
        }
        return data;
    }

    private static long ReadBitField(byte[] data, long bitOffset, int bits, bool signed)
    {
        long value = 0;
        for (int b = 0; b < bits; b++)
        {
            long pos = bitOffset + b;
            int byteIdx = (int)(pos / 8);
            int bitIdx = 7 - (int)(pos % 8);

            if (byteIdx < data.Length)
            {
                int bitVal = (data[byteIdx] >> bitIdx) & 1;
                value = (value << 1) | (uint)bitVal;
            }
            else
            {
                value <<= 1;
            }
        }

        // Sign extend if signed and the MSB is set
        if (signed && bits < 64 && (value & (1L << (bits - 1))) != 0)
        {
            // Sign extend: set all upper bits
            value |= -1L << bits;
        }

        return value;
    }

    private static void WriteBitField(byte[] data, long bitOffset, int bits, long value)
    {
        for (int b = 0; b < bits; b++)
        {
            long pos = bitOffset + b;
            int byteIdx = (int)(pos / 8);
            int bitIdx = 7 - (int)(pos % 8);

            if (byteIdx < data.Length)
            {
                // Write from MSB to LSB of value
                int bitVal = (int)((value >> (bits - 1 - b)) & 1);
                if (bitVal == 1)
                    data[byteIdx] = (byte)(data[byteIdx] | (1 << bitIdx));
                else
                    data[byteIdx] = (byte)(data[byteIdx] & ~(1 << bitIdx));
            }
        }
    }

    // Ref: https://redis.io/docs/latest/commands/bitfield/
    //   "WRAP: wrap around, both with signed and unsigned integers."
    //   "SAT: uses saturation arithmetic — on underflow the value is set to the minimum,
    //    on overflow to the maximum integer value."
    //   "FAIL: in this mode no operation is performed on overflows or underflows detected.
    //    The corresponding return value is set to NULL to signal the condition to the caller."
    private static long? ApplyOverflow(long value, int bits, bool signed, OverflowMode mode)
    {
        long min, max;
        if (signed)
        {
            min = -(1L << (bits - 1));
            max = (1L << (bits - 1)) - 1;
        }
        else
        {
            min = 0;
            if (bits >= 64)
                max = long.MaxValue;
            else
                max = (1L << bits) - 1;
        }

        bool overflow = value > max || value < min;

        switch (mode)
        {
            case OverflowMode.Wrap:
                return WrapValue(value, bits, signed);

            case OverflowMode.Sat:
                if (value > max) return max;
                if (value < min) return min;
                return value;

            case OverflowMode.Fail:
                if (overflow) return null;
                return value;

            default:
                return WrapValue(value, bits, signed);
        }
    }

    private static long WrapValue(long value, int bits, bool signed)
    {
        if (bits >= 64)
            return value; // No wrapping needed for 64-bit

        if (signed)
        {
            long range = 1L << bits;
            // Map into [0, range) then shift to [-range/2, range/2)
            value = ((value % range) + range) % range;
            if (value >= (range >> 1))
                value -= range;
            return value;
        }
        else
        {
            long mask = (1L << bits) - 1;
            return value & mask;
        }
    }

    #endregion
}
