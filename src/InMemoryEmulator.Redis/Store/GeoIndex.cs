namespace InMemoryEmulator.Redis.Store;

// Ref: https://redis.io/docs/latest/commands/geoadd/
//   "Geospatial items are stored into a sorted set, using a 52-bit geohash as the score."
internal static class GeoIndex
{
    private const double EarthRadiusMeters = 6372797.560856;

    public static double EncodeGeoHash(double longitude, double latitude)
    {
        // Interleave bits of normalized longitude and latitude into 52-bit integer
        var lonBits = QuantizeCoordinate(longitude, -180, 180, 26);
        var latBits = QuantizeCoordinate(latitude, -90, 90, 26);

        ulong interleaved = 0;
        for (int i = 0; i < 26; i++)
        {
            interleaved |= ((lonBits >> (25 - i)) & 1UL) << (51 - 2 * i);
            interleaved |= ((latBits >> (25 - i)) & 1UL) << (50 - 2 * i);
        }

        return BitConverter.Int64BitsToDouble((long)interleaved);
    }

    public static (double Longitude, double Latitude) DecodeGeoHash(double score)
    {
        var interleaved = (ulong)BitConverter.DoubleToInt64Bits(score);

        ulong lonBits = 0, latBits = 0;
        for (int i = 0; i < 26; i++)
        {
            lonBits |= ((interleaved >> (51 - 2 * i)) & 1UL) << (25 - i);
            latBits |= ((interleaved >> (50 - 2 * i)) & 1UL) << (25 - i);
        }

        var longitude = DequantizeCoordinate(lonBits, -180, 180, 26);
        var latitude = DequantizeCoordinate(latBits, -90, 90, 26);
        return (longitude, latitude);
    }

    public static double HaversineDistance(double lon1, double lat1, double lon2, double lat2)
    {
        var dLat = DegreesToRadians(lat2 - lat1);
        var dLon = DegreesToRadians(lon2 - lon1);
        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(DegreesToRadians(lat1)) * Math.Cos(DegreesToRadians(lat2)) *
                Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
        return EarthRadiusMeters * c;
    }

    public static double ConvertFromMeters(double meters, string unit)
    {
        return unit.ToLowerInvariant() switch
        {
            "m" => meters,
            "km" => meters / 1000.0,
            "mi" => meters / 1609.344,
            "ft" => meters / 0.3048,
            _ => meters
        };
    }

    public static double ConvertToMeters(double value, string unit)
    {
        return unit.ToLowerInvariant() switch
        {
            "m" => value,
            "km" => value * 1000.0,
            "mi" => value * 1609.344,
            "ft" => value * 0.3048,
            _ => value
        };
    }

    public static string ToGeoHashString(double longitude, double latitude, int precision = 11)
    {
        const string base32 = "0123456789bcdefghjkmnpqrstuvwxyz";
        double minLon = -180, maxLon = 180;
        double minLat = -90, maxLat = 90;
        bool isLon = true;
        int bits = 0, ch = 0;
        var result = new char[precision];
        int idx = 0;

        while (idx < precision)
        {
            if (isLon)
            {
                var mid = (minLon + maxLon) / 2;
                if (longitude >= mid) { ch |= 1 << (4 - bits); minLon = mid; }
                else { maxLon = mid; }
            }
            else
            {
                var mid = (minLat + maxLat) / 2;
                if (latitude >= mid) { ch |= 1 << (4 - bits); minLat = mid; }
                else { maxLat = mid; }
            }
            isLon = !isLon;
            bits++;
            if (bits == 5)
            {
                result[idx++] = base32[ch];
                bits = 0;
                ch = 0;
            }
        }
        return new string(result);
    }

    private static ulong QuantizeCoordinate(double value, double min, double max, int bits)
    {
        var normalized = (value - min) / (max - min);
        return (ulong)(normalized * ((1UL << bits) - 1));
    }

    private static double DequantizeCoordinate(ulong bits, double min, double max, int numBits)
    {
        return min + (bits + 0.5) / (1UL << numBits) * (max - min);
    }

    private static double DegreesToRadians(double degrees) => degrees * Math.PI / 180.0;
}
