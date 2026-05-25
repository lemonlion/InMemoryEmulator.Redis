using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class BitmapCommandTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public BitmapCommandTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    #region SETBIT / GETBIT

    [Fact]
    public async Task SetBit_and_GetBit_basic()
    {
        // Set bit 7 (last bit of first byte) to 1
        var old = await _db.StringSetBitAsync("bm:basic", 7, true);
        Assert.False(old); // was 0

        var val = await _db.StringGetBitAsync("bm:basic", 7);
        Assert.True(val);
    }

    [Fact]
    public async Task SetBit_returns_old_value()
    {
        await _db.StringSetBitAsync("bm:old", 7, true);
        // Set same bit again — old value should be 1
        var old = await _db.StringSetBitAsync("bm:old", 7, true);
        Assert.True(old);

        // Clear it — old value should be 1
        var old2 = await _db.StringSetBitAsync("bm:old", 7, false);
        Assert.True(old2);

        // Clear again — old value should be 0
        var old3 = await _db.StringSetBitAsync("bm:old", 7, false);
        Assert.False(old3);
    }

    [Fact]
    public async Task SetBit_auto_extends_string()
    {
        // Setting bit at offset 32 requires at least 5 bytes (offsets 0-39)
        await _db.StringSetBitAsync("bm:extend", 32, true);

        var val = await _db.StringGetBitAsync("bm:extend", 32);
        Assert.True(val);

        // Verify intermediate bits are 0
        var val0 = await _db.StringGetBitAsync("bm:extend", 0);
        Assert.False(val0);
    }

    [Fact]
    public async Task GetBit_nonexistent_key_returns_false()
    {
        var val = await _db.StringGetBitAsync("bm:nonexistent", 100);
        Assert.False(val);
    }

    [Fact]
    public async Task GetBit_beyond_string_length_returns_false()
    {
        await _db.StringSetBitAsync("bm:short", 0, true);
        // Bit beyond the 1-byte string
        var val = await _db.StringGetBitAsync("bm:short", 100);
        Assert.False(val);
    }

    [Fact]
    public async Task SetBit_bit_ordering_is_msb_first()
    {
        // Ref: https://redis.io/docs/latest/commands/setbit/
        //   "Bit 0 is the most significant bit of the first byte"
        // Setting bit 0 should set the MSB of byte 0 = 0x80
        await _db.StringSetBitAsync("bm:msb", 0, true);

        // Read back the raw string
        var raw = (byte[]?)(await _db.StringGetAsync("bm:msb"));
        Assert.NotNull(raw);
        Assert.Single(raw);
        Assert.Equal(0x80, raw[0]);
    }

    [Fact]
    public async Task SetBit_bit7_sets_lsb_of_first_byte()
    {
        // Bit 7 is the LSB of byte 0 = 0x01
        await _db.StringSetBitAsync("bm:lsb", 7, true);

        var raw = (byte[]?)(await _db.StringGetAsync("bm:lsb"));
        Assert.NotNull(raw);
        Assert.Single(raw);
        Assert.Equal(0x01, raw[0]);
    }

    [Fact]
    public async Task SetBit_key_type_is_string()
    {
        await _db.StringSetBitAsync("bm:type", 0, true);
        var type = await _db.KeyTypeAsync("bm:type");
        Assert.Equal(RedisType.String, type);
    }

    #endregion

    #region BITCOUNT

    [Fact]
    public async Task BitCount_empty_key_returns_zero()
    {
        var count = await _db.StringBitCountAsync("bm:empty");
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task BitCount_counts_all_set_bits()
    {
        // "foobar" in ASCII
        await _db.StringSetAsync("bm:count", "foobar");
        var count = await _db.StringBitCountAsync("bm:count");
        // f=0x66(4), o=0x6F(6), o=0x6F(6), b=0x62(3), a=0x61(3), r=0x72(4) = 26
        Assert.Equal(26, count);
    }

    [Fact]
    public async Task BitCount_with_byte_range()
    {
        // "foobar"
        await _db.StringSetAsync("bm:countrange", "foobar");

        // Count bits in bytes 0-0 (just 'f' = 0x66 = 4 bits)
        var count = await _db.StringBitCountAsync("bm:countrange", 0, 0);
        Assert.Equal(4, count);

        // Count bits in bytes 1-1 (just 'o' = 0x6F = 6 bits)
        var count2 = await _db.StringBitCountAsync("bm:countrange", 1, 1);
        Assert.Equal(6, count2);
    }

    [Fact]
    public async Task BitCount_with_negative_indices()
    {
        await _db.StringSetAsync("bm:countneg", "foobar");

        // -1 = last byte = 'r' = 0x72 = 4 bits
        var count = await _db.StringBitCountAsync("bm:countneg", -1, -1);
        Assert.Equal(4, count);
    }

    [Fact]
    public async Task BitCount_full_string_of_0xFF()
    {
        // Set a string of all 1-bits
        await _db.StringSetAsync("bm:allones", new byte[] { 0xFF, 0xFF });
        var count = await _db.StringBitCountAsync("bm:allones");
        Assert.Equal(16, count);
    }

    #endregion

    #region BITOP

    [Fact]
    public async Task BitOp_AND()
    {
        // a = 0xFF, 0x0F
        // b = 0xF0, 0xFF
        // AND = 0xF0, 0x0F
        await _db.StringSetAsync("bm:a", new byte[] { 0xFF, 0x0F });
        await _db.StringSetAsync("bm:b", new byte[] { 0xF0, 0xFF });

        var len = await _db.StringBitOperationAsync(Bitwise.And, "bm:dest", new RedisKey[] { "bm:a", "bm:b" });
        Assert.Equal(2, len);

        var result = (byte[]?)(await _db.StringGetAsync("bm:dest"));
        Assert.NotNull(result);
        Assert.Equal(new byte[] { 0xF0, 0x0F }, result);
    }

    [Fact]
    public async Task BitOp_OR()
    {
        await _db.StringSetAsync("bm:or:a", new byte[] { 0xF0, 0x00 });
        await _db.StringSetAsync("bm:or:b", new byte[] { 0x0F, 0x0F });

        var len = await _db.StringBitOperationAsync(Bitwise.Or, "bm:or:dest", new RedisKey[] { "bm:or:a", "bm:or:b" });
        Assert.Equal(2, len);

        var result = (byte[]?)(await _db.StringGetAsync("bm:or:dest"));
        Assert.Equal(new byte[] { 0xFF, 0x0F }, result);
    }

    [Fact]
    public async Task BitOp_XOR()
    {
        await _db.StringSetAsync("bm:xor:a", new byte[] { 0xFF, 0x0F });
        await _db.StringSetAsync("bm:xor:b", new byte[] { 0x0F, 0x0F });

        var len = await _db.StringBitOperationAsync(Bitwise.Xor, "bm:xor:dest", new RedisKey[] { "bm:xor:a", "bm:xor:b" });
        Assert.Equal(2, len);

        var result = (byte[]?)(await _db.StringGetAsync("bm:xor:dest"));
        Assert.Equal(new byte[] { 0xF0, 0x00 }, result);
    }

    [Fact]
    public async Task BitOp_NOT()
    {
        await _db.StringSetAsync("bm:not:src", new byte[] { 0xF0, 0x0F });

        var len = await _db.StringBitOperationAsync(Bitwise.Not, "bm:not:dest", "bm:not:src");
        Assert.Equal(2, len);

        var result = (byte[]?)(await _db.StringGetAsync("bm:not:dest"));
        Assert.Equal(new byte[] { 0x0F, 0xF0 }, result);
    }

    [Fact]
    public async Task BitOp_different_lengths_zero_pads()
    {
        // a = 2 bytes, b = 1 byte. Result should be 2 bytes.
        await _db.StringSetAsync("bm:pad:a", new byte[] { 0xFF, 0xFF });
        await _db.StringSetAsync("bm:pad:b", new byte[] { 0x0F });

        var len = await _db.StringBitOperationAsync(Bitwise.And, "bm:pad:dest", new RedisKey[] { "bm:pad:a", "bm:pad:b" });
        Assert.Equal(2, len);

        var result = (byte[]?)(await _db.StringGetAsync("bm:pad:dest"));
        Assert.NotNull(result);
        Assert.Equal(2, result.Length);
        Assert.Equal(0x0F, result[0]);
        Assert.Equal(0x00, result[1]); // zero-padded
    }

    [Fact]
    public async Task BitOp_nonexistent_source_treated_as_zero()
    {
        await _db.StringSetAsync("bm:exists", new byte[] { 0xFF });

        var len = await _db.StringBitOperationAsync(Bitwise.And, "bm:and:result",
            new RedisKey[] { "bm:exists", "bm:doesnotexist" });
        Assert.Equal(1, len);

        var result = (byte[]?)(await _db.StringGetAsync("bm:and:result"));
        Assert.NotNull(result);
        Assert.Equal(new byte[] { 0x00 }, result);
    }

    #endregion

    #region BITPOS

    [Fact]
    public async Task BitPos_find_first_set_bit()
    {
        // 0x00, 0xFF => first 1-bit is at position 8
        await _db.StringSetAsync("bm:pos1", new byte[] { 0x00, 0xFF });
        var pos = await _db.StringBitPositionAsync("bm:pos1", true);
        Assert.Equal(8, pos);
    }

    [Fact]
    public async Task BitPos_find_first_clear_bit()
    {
        // 0xFF, 0xF0 => first 0-bit is at position 12
        await _db.StringSetAsync("bm:pos0", new byte[] { 0xFF, 0xF0 });
        var pos = await _db.StringBitPositionAsync("bm:pos0", false);
        Assert.Equal(12, pos);
    }

    [Fact]
    public async Task BitPos_nonexistent_key_bit1_returns_neg1()
    {
        var pos = await _db.StringBitPositionAsync("bm:nokey", true);
        Assert.Equal(-1, pos);
    }

    [Fact]
    public async Task BitPos_all_zeros_bit1_returns_neg1()
    {
        await _db.StringSetAsync("bm:zeros", new byte[] { 0x00, 0x00 });
        var pos = await _db.StringBitPositionAsync("bm:zeros", true);
        Assert.Equal(-1, pos);
    }

    [Fact]
    public async Task BitPos_all_ones_bit0_returns_neg1_with_explicit_range()
    {
        // Ref: https://redis.io/docs/latest/commands/bitpos/
        //   "If we look for clear bits and the user specified a range with both start
        //    and end, and no 0 bit is found in the range, the function returns -1."
        // StackExchange.Redis's StringBitPositionAsync(key, false) sends
        // BITPOS key 0 0 -1 (explicit range covering the whole string),
        // so Redis returns -1 when no 0 bit is found.
        await _db.StringSetAsync("bm:ones", new byte[] { 0xFF, 0xFF });
        var pos = await _db.StringBitPositionAsync("bm:ones", false);
        Assert.Equal(-1, pos);
    }

    [Fact]
    public async Task BitPos_with_byte_range()
    {
        // 0x00, 0x00, 0xFF
        await _db.StringSetAsync("bm:posrange", new byte[] { 0x00, 0x00, 0xFF });

        // Search for 1-bit starting from byte 2
        var pos = await _db.StringBitPositionAsync("bm:posrange", true, 2);
        Assert.Equal(16, pos);
    }

    [Fact]
    public async Task BitPos_with_byte_range_start_and_end()
    {
        // 0xFF, 0x00, 0xFF
        await _db.StringSetAsync("bm:posrange2", new byte[] { 0xFF, 0x00, 0xFF });

        // Search for 1-bit in byte 1 only (all zeros) => -1
        var pos = await _db.StringBitPositionAsync("bm:posrange2", true, 1, 1);
        Assert.Equal(-1, pos);
    }

    #endregion

    #region BITFIELD

    [Fact]
    public async Task BitField_Get_unsigned()
    {
        // Set byte to 0xFF (all bits set)
        await _db.StringSetAsync("bm:bf:get", new byte[] { 0xFF });

        // GET u8 0 => should return 255
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:get", "GET", "u8", "0");
        var arr = (RedisResult[])result!;
        Assert.Single(arr);
        Assert.Equal(255, (long)arr[0]);
    }

    [Fact]
    public async Task BitField_Get_signed()
    {
        // Set byte to 0xFF
        await _db.StringSetAsync("bm:bf:getsigned", new byte[] { 0xFF });

        // GET i8 0 => should return -1 (signed interpretation of 0xFF)
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:getsigned", "GET", "i8", "0");
        var arr = (RedisResult[])result!;
        Assert.Single(arr);
        Assert.Equal(-1, (long)arr[0]);
    }

    [Fact]
    public async Task BitField_Set_returns_old_value()
    {
        // SET u8 0 200 on empty key => old value should be 0
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:set", "SET", "u8", "0", "200");
        var arr = (RedisResult[])result!;
        Assert.Single(arr);
        Assert.Equal(0, (long)arr[0]); // old value

        // GET it back
        var result2 = await _db.ExecuteAsync("BITFIELD", "bm:bf:set", "GET", "u8", "0");
        var arr2 = (RedisResult[])result2!;
        Assert.Equal(200, (long)arr2[0]);
    }

    [Fact]
    public async Task BitField_IncrBy_unsigned()
    {
        // INCRBY u8 0 100 on empty key => should return 100
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:incr", "INCRBY", "u8", "0", "100");
        var arr = (RedisResult[])result!;
        Assert.Equal(100, (long)arr[0]);

        // INCRBY again by 200 => wraps: (100+200) mod 256 = 44
        var result2 = await _db.ExecuteAsync("BITFIELD", "bm:bf:incr", "INCRBY", "u8", "0", "200");
        var arr2 = (RedisResult[])result2!;
        Assert.Equal(44, (long)arr2[0]);
    }

    [Fact]
    public async Task BitField_IncrBy_signed_wrap()
    {
        // SET i8 0 to 100
        await _db.ExecuteAsync("BITFIELD", "bm:bf:incrsigned", "SET", "i8", "0", "100");

        // INCRBY i8 0 100 => 200 wraps in signed i8 range [-128, 127] => -56
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:incrsigned", "INCRBY", "i8", "0", "100");
        var arr = (RedisResult[])result!;
        Assert.Equal(-56, (long)arr[0]);
    }

    [Fact]
    public async Task BitField_Overflow_Sat()
    {
        // Use SAT mode: overflow saturates to max value
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:sat",
            "SET", "u8", "0", "200",
            "OVERFLOW", "SAT",
            "INCRBY", "u8", "0", "100");
        var arr = (RedisResult[])result!;
        Assert.Equal(2, arr.Length);
        Assert.Equal(0, (long)arr[0]);   // old value from SET
        Assert.Equal(255, (long)arr[1]); // saturated at 255
    }

    [Fact]
    public async Task BitField_Overflow_Fail()
    {
        // Use FAIL mode: overflow returns nil
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:fail",
            "SET", "u8", "0", "200",
            "OVERFLOW", "FAIL",
            "INCRBY", "u8", "0", "100");
        var arr = (RedisResult[])result!;
        Assert.Equal(2, arr.Length);
        Assert.Equal(0, (long)arr[0]);  // old value from SET
        Assert.True(arr[1].IsNull);     // overflow => nil
    }

    [Fact]
    public async Task BitField_hash_offset()
    {
        // # offset means multiply by encoding width
        // SET u8 #0 1 => offset 0
        // SET u8 #1 2 => offset 8
        // SET u8 #2 3 => offset 16
        await _db.ExecuteAsync("BITFIELD", "bm:bf:hash",
            "SET", "u8", "#0", "1",
            "SET", "u8", "#1", "2",
            "SET", "u8", "#2", "3");

        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:hash",
            "GET", "u8", "#0",
            "GET", "u8", "#1",
            "GET", "u8", "#2");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal(1, (long)arr[0]);
        Assert.Equal(2, (long)arr[1]);
        Assert.Equal(3, (long)arr[2]);
    }

    [Fact]
    public async Task BitField_multiple_operations_in_one_call()
    {
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:multi",
            "SET", "u8", "0", "42",
            "GET", "u8", "0",
            "INCRBY", "u8", "0", "10");
        var arr = (RedisResult[])result!;
        Assert.Equal(3, arr.Length);
        Assert.Equal(0, (long)arr[0]);   // old value from SET
        Assert.Equal(42, (long)arr[1]);  // GET reads 42
        Assert.Equal(52, (long)arr[2]);  // INCRBY 42+10=52
    }

    [Fact]
    public async Task BitField_u4_small_encoding()
    {
        // u4: 0-15 range
        await _db.ExecuteAsync("BITFIELD", "bm:bf:u4", "SET", "u4", "0", "15");
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:u4", "GET", "u4", "0");
        var arr = (RedisResult[])result!;
        Assert.Equal(15, (long)arr[0]);
    }

    [Fact]
    public async Task BitField_i16_signed()
    {
        // i16 range: -32768 to 32767
        await _db.ExecuteAsync("BITFIELD", "bm:bf:i16", "SET", "i16", "0", "-1000");
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:i16", "GET", "i16", "0");
        var arr = (RedisResult[])result!;
        Assert.Equal(-1000, (long)arr[0]);
    }

    #endregion

    #region BITFIELD_RO

    [Fact]
    public async Task BitFieldRo_Get_works()
    {
        await _db.StringSetAsync("bm:bfro", new byte[] { 0x2A }); // 42 decimal

        var result = await _db.ExecuteAsync("BITFIELD_RO", "bm:bfro", "GET", "u8", "0");
        var arr = (RedisResult[])result!;
        Assert.Single(arr);
        Assert.Equal(42, (long)arr[0]);
    }

    [Fact]
    public async Task BitFieldRo_nonexistent_key_returns_zero()
    {
        var result = await _db.ExecuteAsync("BITFIELD_RO", "bm:bfro:nope", "GET", "u8", "0");
        var arr = (RedisResult[])result!;
        Assert.Single(arr);
        Assert.Equal(0, (long)arr[0]);
    }

    #endregion

    #region Edge cases

    [Fact]
    public async Task SetBit_on_string_value_works()
    {
        // Bitmap operations work on strings — setting a bit on a normal string key should work
        await _db.StringSetAsync("bm:strval", "A"); // 'A' = 0x41 = 01000001
        var old = await _db.StringSetBitAsync("bm:strval", 0, true);
        // Bit 0 of 'A' (0x41) is 0
        Assert.False(old);

        // Now the byte is 0xC1 = 11000001
        var raw = (byte[]?)(await _db.StringGetAsync("bm:strval"));
        Assert.NotNull(raw);
        Assert.Equal(0xC1, raw[0]);
    }

    [Fact]
    public async Task BitCount_single_byte_0xFF()
    {
        await _db.StringSetAsync("bm:ff", new byte[] { 0xFF });
        var count = await _db.StringBitCountAsync("bm:ff");
        Assert.Equal(8, count);
    }

    [Fact]
    public async Task BitCount_single_byte_0x00()
    {
        await _db.StringSetAsync("bm:00", new byte[] { 0x00 });
        var count = await _db.StringBitCountAsync("bm:00");
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task BitOp_AND_multiple_keys()
    {
        await _db.StringSetAsync("bm:m:a", new byte[] { 0xFF });
        await _db.StringSetAsync("bm:m:b", new byte[] { 0x0F });
        await _db.StringSetAsync("bm:m:c", new byte[] { 0x03 });

        var len = await _db.StringBitOperationAsync(Bitwise.And, "bm:m:dest",
            new RedisKey[] { "bm:m:a", "bm:m:b", "bm:m:c" });
        Assert.Equal(1, len);

        var result = (byte[]?)(await _db.StringGetAsync("bm:m:dest"));
        Assert.Equal(new byte[] { 0x03 }, result);
    }

    [Fact]
    public async Task BitField_Overflow_Sat_signed_underflow()
    {
        // i8 range: -128 to 127
        // Set to -100, then INCRBY -100 => -200 saturates to -128
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:satunder",
            "SET", "i8", "0", "-100",
            "OVERFLOW", "SAT",
            "INCRBY", "i8", "0", "-100");
        var arr = (RedisResult[])result!;
        Assert.Equal(2, arr.Length);
        Assert.Equal(0, (long)arr[0]);     // old value from SET
        Assert.Equal(-128, (long)arr[1]);  // saturated at -128
    }

    [Fact]
    public async Task BitField_Get_nonexistent_key_returns_zero()
    {
        var result = await _db.ExecuteAsync("BITFIELD", "bm:bf:nope", "GET", "u8", "0");
        var arr = (RedisResult[])result!;
        Assert.Single(arr);
        Assert.Equal(0, (long)arr[0]);
    }

    [Fact]
    public async Task SetBit_multiple_bits_in_same_byte()
    {
        // Set bits 0, 1, 2, 3 in first byte
        await _db.StringSetBitAsync("bm:multi", 0, true);
        await _db.StringSetBitAsync("bm:multi", 1, true);
        await _db.StringSetBitAsync("bm:multi", 2, true);
        await _db.StringSetBitAsync("bm:multi", 3, true);

        var raw = (byte[]?)(await _db.StringGetAsync("bm:multi"));
        Assert.NotNull(raw);
        Assert.Equal(0xF0, raw[0]); // 11110000
    }

    [Fact]
    public async Task BitPos_first_bit_of_string()
    {
        // 0x80 = 10000000 => first 1-bit is at position 0
        await _db.StringSetAsync("bm:firstbit", new byte[] { 0x80 });
        var pos = await _db.StringBitPositionAsync("bm:firstbit", true);
        Assert.Equal(0, pos);
    }

    [Fact]
    public async Task BitPos_last_bit_of_byte()
    {
        // 0x01 = 00000001 => first 1-bit is at position 7
        await _db.StringSetAsync("bm:lastbit", new byte[] { 0x01 });
        var pos = await _db.StringBitPositionAsync("bm:lastbit", true);
        Assert.Equal(7, pos);
    }

    #endregion
}
