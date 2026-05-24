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

    private static ValueTask<RespValue> GeoSearchStore(CommandContext ctx)
    {
        // Simplified: store results into destination sorted set
        var destKey = ctx.GetArgString(0);
        var srcKey = ctx.GetArgString(1);
        // Re-route to GeoSearch logic but store results
        // For now, return 0 as this is complex
        return ValueTask.FromResult(RespValue.Zero);
    }
}
