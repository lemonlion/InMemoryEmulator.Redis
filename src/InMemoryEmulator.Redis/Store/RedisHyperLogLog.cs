using System.Security.Cryptography;
using System.Text;

namespace InMemoryEmulator.Redis.Store;

// Ref: https://redis.io/docs/latest/develop/data-types/probabilistic/hyperloglogs/
//   "HyperLogLog is a probabilistic data structure that estimates the cardinality of a set."
//   Uses 16384 registers (2^14), each 6 bits.
internal sealed class RedisHyperLogLog : RedisEntry
{
    private const int NumRegisters = 16384; // 2^14
    private readonly byte[] _registers = new byte[NumRegisters];

    public override string TypeName => "string"; // Redis reports HLL as string type

    public bool Add(byte[] element)
    {
        var hash = ComputeHash(element);
        var index = (int)(hash & 0x3FFF); // lower 14 bits
        var remaining = hash >> 14;
        var runLength = CountLeadingZeros(remaining) + 1;

        if (runLength > _registers[index])
        {
            _registers[index] = (byte)runLength;
            return true;
        }
        return false;
    }

    public long Count()
    {
        // Standard HyperLogLog cardinality estimation
        double sum = 0;
        int zeros = 0;
        for (int i = 0; i < NumRegisters; i++)
        {
            sum += 1.0 / (1L << _registers[i]);
            if (_registers[i] == 0) zeros++;
        }

        double alpha = 0.7213 / (1 + 1.079 / NumRegisters);
        double estimate = alpha * NumRegisters * NumRegisters / sum;

        // Small range correction
        if (estimate <= 2.5 * NumRegisters && zeros > 0)
            estimate = NumRegisters * Math.Log((double)NumRegisters / zeros);

        return (long)Math.Round(estimate);
    }

    public void MergeWith(RedisHyperLogLog other)
    {
        for (int i = 0; i < NumRegisters; i++)
            _registers[i] = Math.Max(_registers[i], other._registers[i]);
    }

    public byte[] GetRegisters() => _registers;

    private static ulong ComputeHash(byte[] data)
    {
        var hash = SHA256.HashData(data);
        return BitConverter.ToUInt64(hash, 0);
    }

    private static int CountLeadingZeros(ulong value)
    {
        if (value == 0) return 50; // cap at 50 for 6-bit register
        int count = 0;
        while ((value & 1) == 0 && count < 50)
        {
            count++;
            value >>= 1;
        }
        return count;
    }
}
