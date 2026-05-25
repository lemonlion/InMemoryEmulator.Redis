using System.Globalization;
using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class GeoCommands : ICommandHandler
{
    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "GEOADD" => GeoAdd(context),
            "GEODIST" => GeoDist(context),
            "GEOHASH" => GeoHash(context),
            "GEOPOS" => GeoPos(context),
            "GEOSEARCH" => GeoSearch(context),
            "GEOSEARCHSTORE" => GeoSearchStore(context),
            "GEORADIUS" => GeoRadius(context),
            "GEORADIUS_RO" => GeoRadius(context),
            "GEORADIUSBYMEMBER" => GeoRadiusByMember(context),
            "GEORADIUSBYMEMBER_RO" => GeoRadiusByMember(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static RedisSortedSet GetOrCreate(CommandContext ctx, string key)
    {
        var entry = ctx.Database.GetEntry(key);
        if (entry == null) { var z = new RedisSortedSet(); ctx.Database.SetEntry(key, z); return z; }
        if (entry is not RedisSortedSet zs) throw new WrongTypeException();
        return zs;
    }

    private static ValueTask<RespValue> GeoAdd(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/geoadd/
        var key = ctx.GetArgString(0);
        var zset = GetOrCreate(ctx, key);

        bool nx = false, xx = false, ch = false;
        int i = 1;
        while (i < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            if (opt == "NX") { nx = true; i++; }
            else if (opt == "XX") { xx = true; i++; }
            else if (opt == "CH") { ch = true; i++; }
            else break;
        }

        int added = 0, changed = 0;
        while (i + 2 < ctx.Arguments.Length)
        {
            var lon = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
            var lat = double.Parse(ctx.GetArgString(i + 1), CultureInfo.InvariantCulture);
            var member = ctx.GetArgString(i + 2);
            i += 3;

            var score = GeoIndex.EncodeGeoHash(lon, lat);
            var existing = zset.GetScore(member);

            if (nx && existing.HasValue) continue;
            if (xx && !existing.HasValue) continue;

            if (existing.HasValue)
            {
                if (score != existing.Value) { zset.Add(member, score); changed++; }
            }
            else
            {
                zset.Add(member, score);
                added++;
            }
        }

        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(ch ? added + changed : added));
    }

    private static ValueTask<RespValue> GeoDist(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/geodist/
        var key = ctx.GetArgString(0);
        var member1 = ctx.GetArgString(1);
        var member2 = ctx.GetArgString(2);
        var unit = ctx.Arguments.Length > 3 ? ctx.GetArgString(3) : "m";

        var zset = ctx.Database.GetTyped<RedisSortedSet>(key);
        if (zset == null) return ValueTask.FromResult(RespValue.NullBulkString);

        var score1 = zset.GetScore(member1);
        var score2 = zset.GetScore(member2);
        if (!score1.HasValue || !score2.HasValue)
            return ValueTask.FromResult(RespValue.NullBulkString);

        var (lon1, lat1) = GeoIndex.DecodeGeoHash(score1.Value);
        var (lon2, lat2) = GeoIndex.DecodeGeoHash(score2.Value);
        var dist = GeoIndex.HaversineDistance(lon1, lat1, lon2, lat2);
        dist = GeoIndex.ConvertFromMeters(dist, unit);

        return ValueTask.FromResult<RespValue>(
            RespValue.FromBulkString(dist.ToString("F4", CultureInfo.InvariantCulture)));
    }

    private static ValueTask<RespValue> GeoHash(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/geohash/
        var key = ctx.GetArgString(0);
        var zset = ctx.Database.GetTyped<RedisSortedSet>(key);
        var results = new RespValue[ctx.Arguments.Length - 1];

        for (int i = 1; i < ctx.Arguments.Length; i++)
        {
            var member = ctx.GetArgString(i);
            var score = zset?.GetScore(member);
            if (!score.HasValue)
            {
                results[i - 1] = RespValue.NullBulkString;
                continue;
            }
            var (lon, lat) = GeoIndex.DecodeGeoHash(score.Value);
            results[i - 1] = RespValue.FromBulkString(GeoIndex.ToGeoHashString(lon, lat));
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> GeoPos(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/geopos/
        var key = ctx.GetArgString(0);
        var zset = ctx.Database.GetTyped<RedisSortedSet>(key);
        var results = new RespValue[ctx.Arguments.Length - 1];

        for (int i = 1; i < ctx.Arguments.Length; i++)
        {
            var member = ctx.GetArgString(i);
            var score = zset?.GetScore(member);
            if (!score.HasValue)
            {
                results[i - 1] = RespValue.NullArray;
                continue;
            }
            var (lon, lat) = GeoIndex.DecodeGeoHash(score.Value);
            results[i - 1] = new RespValue.Array(new RespValue[]
            {
                RespValue.FromBulkString(lon.ToString("G17", CultureInfo.InvariantCulture)),
                RespValue.FromBulkString(lat.ToString("G17", CultureInfo.InvariantCulture))
            });
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results));
    }

    private static ValueTask<RespValue> GeoSearch(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/geosearch/
        var key = ctx.GetArgString(0);
        var zset = ctx.Database.GetTyped<RedisSortedSet>(key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);

        double centerLon = 0, centerLat = 0;
        double radius = 0;
        string unit = "m";
        bool withCoord = false, withDist = false, withHash = false;
        bool asc = true;
        int count = int.MaxValue;

        int i = 1;
        while (i < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "FROMMEMBER":
                    i++;
                    var member = ctx.GetArgString(i);
                    var memberScore = zset.GetScore(member);
                    if (memberScore.HasValue)
                        (centerLon, centerLat) = GeoIndex.DecodeGeoHash(memberScore.Value);
                    break;
                case "FROMLONLAT":
                    i++;
                    centerLon = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
                    i++;
                    centerLat = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
                    break;
                case "BYRADIUS":
                    i++;
                    radius = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
                    i++;
                    unit = ctx.GetArgString(i);
                    break;
                case "BYBOX":
                    i++;
                    var width = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
                    i++;
                    var height = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
                    i++;
                    unit = ctx.GetArgString(i);
                    radius = GeoIndex.ConvertToMeters(Math.Max(width, height) / 2, unit);
                    unit = "m";
                    break;
                case "WITHCOORD": withCoord = true; break;
                case "WITHDIST": withDist = true; break;
                case "WITHHASH": withHash = true; break;
                case "ASC": asc = true; break;
                case "DESC": asc = false; break;
                case "COUNT":
                    i++;
                    count = (int)long.Parse(ctx.GetArgString(i));
                    break;
                case "ANY": break; // ignore for simplicity
            }
            i++;
        }

        var radiusMeters = GeoIndex.ConvertToMeters(radius, unit);
        var matches = new List<(string Member, double Dist, double Lon, double Lat, double Score)>();

        foreach (var (score, memberName) in zset.ScoreIndex)
        {
            var (lon, lat) = GeoIndex.DecodeGeoHash(score);
            var dist = GeoIndex.HaversineDistance(centerLon, centerLat, lon, lat);
            if (dist <= radiusMeters)
                matches.Add((memberName, dist, lon, lat, score));
        }

        if (asc) matches.Sort((a, b) => a.Dist.CompareTo(b.Dist));
        else matches.Sort((a, b) => b.Dist.CompareTo(a.Dist));

        if (count < matches.Count) matches = matches.Take(count).ToList();

        bool hasExtras = withCoord || withDist || withHash;
        var results = new List<RespValue>();

        foreach (var (memberName, dist, lon, lat, score) in matches)
        {
            if (!hasExtras)
            {
                results.Add(RespValue.FromBulkString(memberName));
            }
            else
            {
                var item = new List<RespValue> { RespValue.FromBulkString(memberName) };
                if (withDist)
                    item.Add(RespValue.FromBulkString(
                        GeoIndex.ConvertFromMeters(dist, unit).ToString("F4", CultureInfo.InvariantCulture)));
                if (withHash)
                    item.Add(new RespValue.Integer(BitConverter.DoubleToInt64Bits(score)));
                if (withCoord)
                    item.Add(new RespValue.Array(new RespValue[]
                    {
                        RespValue.FromBulkString(lon.ToString("G17", CultureInfo.InvariantCulture)),
                        RespValue.FromBulkString(lat.ToString("G17", CultureInfo.InvariantCulture))
                    }));
                results.Add(new RespValue.Array(item.ToArray()));
            }
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }

    // Ref: https://redis.io/docs/latest/commands/geosearchstore/
    private static ValueTask<RespValue> GeoSearchStore(CommandContext ctx)
    {
        var destKey = ctx.GetArgString(0);
        var srcKey = ctx.GetArgString(1);
        var zset = ctx.Database.GetTyped<RedisSortedSet>(srcKey);
        if (zset == null)
        {
            ctx.Database.RemoveEntry(destKey);
            return ValueTask.FromResult(RespValue.Zero);
        }

        double centerLon = 0, centerLat = 0;
        double radius = 0;
        string unit = "m";
        bool storeDist = false;
        int count = int.MaxValue;
        bool asc = true;

        int i = 2;
        while (i < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "FROMMEMBER":
                    i++;
                    var member = ctx.GetArgString(i);
                    var memberScore = zset.GetScore(member);
                    if (memberScore.HasValue)
                        (centerLon, centerLat) = GeoIndex.DecodeGeoHash(memberScore.Value);
                    break;
                case "FROMLONLAT":
                    i++;
                    centerLon = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
                    i++;
                    centerLat = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
                    break;
                case "BYRADIUS":
                    i++;
                    radius = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
                    i++;
                    unit = ctx.GetArgString(i);
                    break;
                case "BYBOX":
                    i++;
                    var width = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
                    i++;
                    var height = double.Parse(ctx.GetArgString(i), CultureInfo.InvariantCulture);
                    i++;
                    unit = ctx.GetArgString(i);
                    radius = GeoIndex.ConvertToMeters(Math.Max(width, height) / 2, unit);
                    unit = "m";
                    break;
                case "ASC": asc = true; break;
                case "DESC": asc = false; break;
                case "COUNT": i++; count = (int)long.Parse(ctx.GetArgString(i)); break;
                case "STOREDIST": storeDist = true; break;
                case "ANY": break;
            }
            i++;
        }

        var radiusMeters = GeoIndex.ConvertToMeters(radius, unit);
        var matches = new List<(string Member, double Dist, double Score)>();

        foreach (var (score, memberName) in zset.ScoreIndex)
        {
            var (lon, lat) = GeoIndex.DecodeGeoHash(score);
            var dist = GeoIndex.HaversineDistance(centerLon, centerLat, lon, lat);
            if (dist <= radiusMeters)
                matches.Add((memberName, dist, score));
        }

        if (asc) matches.Sort((a, b) => a.Dist.CompareTo(b.Dist));
        else matches.Sort((a, b) => b.Dist.CompareTo(a.Dist));
        if (count < matches.Count) matches = matches.Take(count).ToList();

        if (matches.Count == 0)
        {
            ctx.Database.RemoveEntry(destKey);
            return ValueTask.FromResult(RespValue.Zero);
        }

        var dest = new RedisSortedSet();
        foreach (var (memberName, dist, score) in matches)
            dest.Add(memberName, storeDist ? GeoIndex.ConvertFromMeters(dist, unit) : score);
        ctx.Database.SetEntry(destKey, dest);

        return ValueTask.FromResult<RespValue>(new RespValue.Integer(matches.Count));
    }

    // Ref: https://redis.io/docs/latest/commands/georadius/
    //   "Return the members of a sorted set populated with geospatial information using GEOADD,
    //    which are within the borders of the area specified with the center location and the
    //    maximum distance from the center (the radius)."
    //   Deprecated since Redis 6.2, replaced by GEOSEARCH.
    //   Syntax: GEORADIUS key longitude latitude radius m|km|ft|mi
    //           [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
    private static ValueTask<RespValue> GeoRadius(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = ctx.Database.GetTyped<RedisSortedSet>(key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var centerLon = double.Parse(ctx.GetArgString(1), CultureInfo.InvariantCulture);
        var centerLat = double.Parse(ctx.GetArgString(2), CultureInfo.InvariantCulture);
        var radius = double.Parse(ctx.GetArgString(3), CultureInfo.InvariantCulture);
        var unit = ctx.GetArgString(4);

        bool withCoord = false, withDist = false, withHash = false;
        bool asc = true;
        int count = int.MaxValue;
        string? storeKey = null, storeDistKey = null;

        int i = 5;
        while (i < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "WITHCOORD": withCoord = true; break;
                case "WITHDIST": withDist = true; break;
                case "WITHHASH": withHash = true; break;
                case "ASC": asc = true; break;
                case "DESC": asc = false; break;
                case "COUNT":
                    i++;
                    count = (int)long.Parse(ctx.GetArgString(i));
                    // Check for ANY flag
                    if (i + 1 < ctx.Arguments.Length && ctx.GetArgString(i + 1).Equals("ANY", StringComparison.OrdinalIgnoreCase))
                        i++;
                    break;
                case "STORE": i++; storeKey = ctx.GetArgString(i); break;
                case "STOREDIST": i++; storeDistKey = ctx.GetArgString(i); break;
            }
            i++;
        }

        return GeoRadiusCore(ctx, zset, key, centerLon, centerLat, radius, unit,
            withCoord, withDist, withHash, asc, count, storeKey, storeDistKey);
    }

    // Ref: https://redis.io/docs/latest/commands/georadiusbymember/
    //   "This command is exactly like GEORADIUS with the sole difference that instead of taking,
    //    as the center of the area to query, a longitude and latitude value, it takes the name
    //    of a member already existing inside the geospatial index."
    //   Deprecated since Redis 6.2, replaced by GEOSEARCH with FROMMEMBER.
    //   Syntax: GEORADIUSBYMEMBER key member radius m|km|ft|mi [same options as GEORADIUS]
    private static ValueTask<RespValue> GeoRadiusByMember(CommandContext ctx)
    {
        var key = ctx.GetArgString(0);
        var zset = ctx.Database.GetTyped<RedisSortedSet>(key);
        if (zset == null) return ValueTask.FromResult(RespValue.EmptyArray);

        var member = ctx.GetArgString(1);
        var memberScore = zset.GetScore(member);
        if (!memberScore.HasValue)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "could not decode requested zset member"));

        var (centerLon, centerLat) = GeoIndex.DecodeGeoHash(memberScore.Value);
        var radius = double.Parse(ctx.GetArgString(2), CultureInfo.InvariantCulture);
        var unit = ctx.GetArgString(3);

        bool withCoord = false, withDist = false, withHash = false;
        bool asc = true;
        int count = int.MaxValue;
        string? storeKey = null, storeDistKey = null;

        int i = 4;
        while (i < ctx.Arguments.Length)
        {
            var opt = ctx.GetArgString(i).ToUpperInvariant();
            switch (opt)
            {
                case "WITHCOORD": withCoord = true; break;
                case "WITHDIST": withDist = true; break;
                case "WITHHASH": withHash = true; break;
                case "ASC": asc = true; break;
                case "DESC": asc = false; break;
                case "COUNT":
                    i++;
                    count = (int)long.Parse(ctx.GetArgString(i));
                    if (i + 1 < ctx.Arguments.Length && ctx.GetArgString(i + 1).Equals("ANY", StringComparison.OrdinalIgnoreCase))
                        i++;
                    break;
                case "STORE": i++; storeKey = ctx.GetArgString(i); break;
                case "STOREDIST": i++; storeDistKey = ctx.GetArgString(i); break;
            }
            i++;
        }

        return GeoRadiusCore(ctx, zset, key, centerLon, centerLat, radius, unit,
            withCoord, withDist, withHash, asc, count, storeKey, storeDistKey);
    }

    private static ValueTask<RespValue> GeoRadiusCore(CommandContext ctx, RedisSortedSet zset, string key,
        double centerLon, double centerLat, double radius, string unit,
        bool withCoord, bool withDist, bool withHash,
        bool asc, int count, string? storeKey, string? storeDistKey)
    {
        var radiusMeters = GeoIndex.ConvertToMeters(radius, unit);
        var matches = new List<(string Member, double Dist, double Lon, double Lat, double Score)>();

        foreach (var (score, memberName) in zset.ScoreIndex)
        {
            var (lon, lat) = GeoIndex.DecodeGeoHash(score);
            var dist = GeoIndex.HaversineDistance(centerLon, centerLat, lon, lat);
            if (dist <= radiusMeters)
                matches.Add((memberName, dist, lon, lat, score));
        }

        if (asc) matches.Sort((a, b) => a.Dist.CompareTo(b.Dist));
        else matches.Sort((a, b) => b.Dist.CompareTo(a.Dist));

        if (count < matches.Count) matches = matches.Take(count).ToList();

        // STORE or STOREDIST: write to a sorted set and return count
        if (storeKey != null || storeDistKey != null)
        {
            var destKey = storeKey ?? storeDistKey!;
            bool storeDist = storeDistKey != null;
            if (matches.Count == 0)
            {
                ctx.Database.RemoveEntry(destKey);
                return ValueTask.FromResult(RespValue.Zero);
            }
            var dest = new RedisSortedSet();
            foreach (var (memberName, dist, lon, lat, score) in matches)
                dest.Add(memberName, storeDist ? GeoIndex.ConvertFromMeters(dist, unit) : score);
            ctx.Database.SetEntry(destKey, dest);
            return ValueTask.FromResult<RespValue>(new RespValue.Integer(matches.Count));
        }

        bool hasExtras = withCoord || withDist || withHash;
        var results = new List<RespValue>();

        foreach (var (memberName, dist, lon, lat, score) in matches)
        {
            if (!hasExtras)
            {
                results.Add(RespValue.FromBulkString(memberName));
            }
            else
            {
                var item = new List<RespValue> { RespValue.FromBulkString(memberName) };
                if (withDist)
                    item.Add(RespValue.FromBulkString(
                        GeoIndex.ConvertFromMeters(dist, unit).ToString("F4", CultureInfo.InvariantCulture)));
                if (withHash)
                    item.Add(new RespValue.Integer(BitConverter.DoubleToInt64Bits(score)));
                if (withCoord)
                    item.Add(new RespValue.Array(new RespValue[]
                    {
                        RespValue.FromBulkString(lon.ToString("G17", CultureInfo.InvariantCulture)),
                        RespValue.FromBulkString(lat.ToString("G17", CultureInfo.InvariantCulture))
                    }));
                results.Add(new RespValue.Array(item.ToArray()));
            }
        }

        return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
    }
}
