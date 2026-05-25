using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class GeoCommandGapTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public GeoCommandGapTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // ═══════════════════════════════════════════════════════════════
    // GEOHASH tests
    // Ref: https://redis.io/docs/latest/commands/geohash/
    //   "Return valid Geohash strings representing the position of one
    //    or more elements in a sorted set value representing a
    //    geospatial index."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoHash_returns_hash_string_for_member()
    {
        await _db.GeoAddAsync("geohash_key", new GeoEntry(13.361389, 38.115556, "Palermo"));
        var hashes = await _db.GeoHashAsync("geohash_key", new RedisValue[] { "Palermo" });
        Assert.Single(hashes);
        Assert.NotNull(hashes[0]);
        // Geohash is an 11-character string
        Assert.Equal(11, hashes[0]!.Length);
    }

    [Fact]
    public async Task GeoHash_returns_null_for_nonexistent_member()
    {
        await _db.GeoAddAsync("geohash_key2", new GeoEntry(13.361389, 38.115556, "Palermo"));
        var hashes = await _db.GeoHashAsync("geohash_key2", new RedisValue[] { "NonExistent" });
        Assert.Single(hashes);
        Assert.Null(hashes[0]);
    }

    [Fact]
    public async Task GeoHash_returns_multiple_hashes()
    {
        await _db.GeoAddAsync("geohash_multi", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
        });
        var hashes = await _db.GeoHashAsync("geohash_multi", new RedisValue[] { "Palermo", "Catania" });
        Assert.Equal(2, hashes.Length);
        Assert.NotNull(hashes[0]);
        Assert.NotNull(hashes[1]);
        // Each hash should be unique
        Assert.NotEqual(hashes[0], hashes[1]);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEOSEARCH FROMMEMBER BYRADIUS tests
    // Ref: https://redis.io/docs/latest/commands/geosearch/
    //   "Return the members of a sorted set populated with geospatial
    //    information using GEOADD, which are within the borders of the
    //    area specified by a given shape."
    //   "FROMMEMBER: Use the position of the given existing member in
    //    the sorted set."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoSearch_FromMember_ByRadius_returns_nearby_members()
    {
        await _db.GeoAddAsync("geosearch_member", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(2.349014, 48.864716, "Paris"),
        });

        var results = await _db.GeoSearchAsync("geosearch_member", "Palermo",
            new GeoSearchCircle(200, GeoUnit.Kilometers));
        Assert.NotNull(results);
        var names = results.Select(r => r.Member.ToString()).ToArray();
        Assert.Contains("Palermo", names);
        Assert.Contains("Catania", names);
        Assert.DoesNotContain("Paris", names);
    }

    [Fact]
    public async Task GeoSearch_FromMember_ByRadius_includes_self()
    {
        await _db.GeoAddAsync("geosearch_self", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
        });

        var results = await _db.GeoSearchAsync("geosearch_self", "Palermo",
            new GeoSearchCircle(1, GeoUnit.Kilometers));
        Assert.NotNull(results);
        var names = results.Select(r => r.Member.ToString()).ToArray();
        Assert.Contains("Palermo", names);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEOSEARCH FROMLONLAT BYRADIUS tests
    // Ref: https://redis.io/docs/latest/commands/geosearch/
    //   "FROMLONLAT: Use the given longitude and latitude position."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoSearch_FromLonLat_ByRadius_returns_nearby_members()
    {
        await _db.GeoAddAsync("geosearch_lonlat", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(2.349014, 48.864716, "Paris"),
        });

        var results = await _db.GeoSearchAsync("geosearch_lonlat", 13.361389, 38.115556,
            new GeoSearchCircle(200, GeoUnit.Kilometers));
        Assert.NotNull(results);
        var names = results.Select(r => r.Member.ToString()).ToArray();
        Assert.Contains("Palermo", names);
        Assert.Contains("Catania", names);
        Assert.DoesNotContain("Paris", names);
    }

    [Fact]
    public async Task GeoSearch_FromLonLat_empty_key_returns_empty()
    {
        var results = await _db.GeoSearchAsync("geosearch_empty", 0, 0,
            new GeoSearchCircle(100, GeoUnit.Kilometers));
        Assert.NotNull(results);
        Assert.Empty(results);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEOSEARCH BYBOX tests
    // Ref: https://redis.io/docs/latest/commands/geosearch/
    //   "BYBOX: Search within a rectangular area."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoSearch_ByBox_returns_members_within_box()
    {
        await _db.GeoAddAsync("geosearch_box", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(2.349014, 48.864716, "Paris"),
        });

        // A box wide enough to cover Sicily but not Paris
        var results = await _db.GeoSearchAsync("geosearch_box", "Palermo",
            new GeoSearchBox(500, 400, GeoUnit.Kilometers));
        Assert.NotNull(results);
        var names = results.Select(r => r.Member.ToString()).ToArray();
        Assert.Contains("Palermo", names);
        Assert.Contains("Catania", names);
        Assert.DoesNotContain("Paris", names);
    }

    [Fact]
    public async Task GeoSearch_ByBox_FromLonLat()
    {
        await _db.GeoAddAsync("geosearch_box_ll", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(2.349014, 48.864716, "Paris"),
        });

        var results = await _db.GeoSearchAsync("geosearch_box_ll", 13.361389, 38.115556,
            new GeoSearchBox(500, 400, GeoUnit.Kilometers));
        Assert.NotNull(results);
        var names = results.Select(r => r.Member.ToString()).ToArray();
        Assert.Contains("Palermo", names);
        Assert.Contains("Catania", names);
        Assert.DoesNotContain("Paris", names);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEOSEARCH with COUNT tests
    // Ref: https://redis.io/docs/latest/commands/geosearch/
    //   "COUNT count: limit the results to the first N matching items."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoSearch_with_count_limits_results()
    {
        await _db.GeoAddAsync("geosearch_count", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(14.26812, 40.85216, "Naples"),
        });

        var results = await _db.GeoSearchAsync("geosearch_count", "Palermo",
            new GeoSearchCircle(500, GeoUnit.Kilometers), count: 2);
        Assert.NotNull(results);
        Assert.Equal(2, results.Length);
    }

    [Fact]
    public async Task GeoSearch_with_count_one_returns_single_result()
    {
        await _db.GeoAddAsync("geosearch_count1", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
        });

        var results = await _db.GeoSearchAsync("geosearch_count1", "Palermo",
            new GeoSearchCircle(500, GeoUnit.Kilometers), count: 1);
        Assert.NotNull(results);
        Assert.Single(results);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEOSEARCH with ASC/DESC tests
    // Ref: https://redis.io/docs/latest/commands/geosearch/
    //   "ASC: Sort returned items from the nearest to the farthest."
    //   "DESC: Sort returned items from the farthest to the nearest."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoSearch_asc_sorts_nearest_first()
    {
        await _db.GeoAddAsync("geosearch_asc", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(14.26812, 40.85216, "Naples"),
        });

        var results = await _db.GeoSearchAsync("geosearch_asc", "Palermo",
            new GeoSearchCircle(500, GeoUnit.Kilometers), order: Order.Ascending);
        Assert.NotNull(results);
        Assert.True(results.Length >= 3);
        // Palermo should be first (distance 0)
        Assert.Equal("Palermo", results[0].Member.ToString());
    }

    [Fact]
    public async Task GeoSearch_desc_sorts_farthest_first()
    {
        await _db.GeoAddAsync("geosearch_desc", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(14.26812, 40.85216, "Naples"),
        });

        var results = await _db.GeoSearchAsync("geosearch_desc", "Palermo",
            new GeoSearchCircle(500, GeoUnit.Kilometers), order: Order.Descending);
        Assert.NotNull(results);
        Assert.True(results.Length >= 3);
        // Palermo should be last (distance 0)
        Assert.Equal("Palermo", results[results.Length - 1].Member.ToString());
    }

    // ═══════════════════════════════════════════════════════════════
    // GEOSEARCHSTORE tests
    // Ref: https://redis.io/docs/latest/commands/geosearchstore/
    //   "This command is like GEOSEARCH, but stores the result in
    //    destination key."
    //   "Returns the number of members added to the destination key."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoSearchStore_stores_results_in_destination()
    {
        await _db.GeoAddAsync("geoss_src", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(2.349014, 48.864716, "Paris"),
        });

        var result = await _db.ExecuteAsync("GEOSEARCHSTORE", "geoss_dst", "geoss_src",
            "FROMLONLAT", "13.361389", "38.115556", "BYRADIUS", "200", "km");
        var count = (long)result;
        Assert.Equal(2, count); // Palermo and Catania

        // Verify dest has the members as a sorted set (geo uses sorted sets)
        var destType = await _db.KeyTypeAsync("geoss_dst");
        Assert.Equal(RedisType.SortedSet, destType);
        var destLen = await _db.SortedSetLengthAsync("geoss_dst");
        Assert.Equal(2, destLen);
    }

    [Fact]
    public async Task GeoSearchStore_with_count_limits_stored_results()
    {
        await _db.GeoAddAsync("geoss_src2", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
            new(14.26812, 40.85216, "Naples"),
        });

        var result = await _db.ExecuteAsync("GEOSEARCHSTORE", "geoss_dst2", "geoss_src2",
            "FROMLONLAT", "13.361389", "38.115556", "BYRADIUS", "500", "km", "COUNT", "2", "ASC");
        var count = (long)result;
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task GeoSearchStore_empty_result_returns_zero()
    {
        await _db.GeoAddAsync("geoss_src3", new GeoEntry(13.361389, 38.115556, "Palermo"));

        var result = await _db.ExecuteAsync("GEOSEARCHSTORE", "geoss_dst3", "geoss_src3",
            "FROMLONLAT", "100.0", "50.0", "BYRADIUS", "1", "km");
        var count = (long)result;
        Assert.Equal(0, count);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEODIST with different units tests
    // Ref: https://redis.io/docs/latest/commands/geodist/
    //   "Return the distance between two members in the geospatial
    //    index represented by the sorted set."
    //   "The unit must be one of: m, km, mi, ft."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoDist_meters_returns_distance()
    {
        await _db.GeoAddAsync("geodist_units", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
        });

        var distM = await _db.GeoDistanceAsync("geodist_units", "Palermo", "Catania", GeoUnit.Meters);
        Assert.NotNull(distM);
        // Approximately 166 km = 166000 m
        Assert.InRange(distM!.Value, 100000, 250000);
    }

    [Fact]
    public async Task GeoDist_kilometers_returns_distance()
    {
        await _db.GeoAddAsync("geodist_km", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
        });

        var distKm = await _db.GeoDistanceAsync("geodist_km", "Palermo", "Catania", GeoUnit.Kilometers);
        Assert.NotNull(distKm);
        Assert.InRange(distKm!.Value, 100, 250);
    }

    [Fact]
    public async Task GeoDist_miles_returns_distance()
    {
        await _db.GeoAddAsync("geodist_mi", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
        });

        var distMi = await _db.GeoDistanceAsync("geodist_mi", "Palermo", "Catania", GeoUnit.Miles);
        Assert.NotNull(distMi);
        // ~166 km = ~103 miles
        Assert.InRange(distMi!.Value, 60, 160);
    }

    [Fact]
    public async Task GeoDist_feet_returns_distance()
    {
        await _db.GeoAddAsync("geodist_ft", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
        });

        var distFt = await _db.GeoDistanceAsync("geodist_ft", "Palermo", "Catania", GeoUnit.Feet);
        Assert.NotNull(distFt);
        // ~166 km = ~544619 feet
        Assert.InRange(distFt!.Value, 300000, 800000);
    }

    [Fact]
    public async Task GeoDist_nonexistent_member_returns_null()
    {
        await _db.GeoAddAsync("geodist_null", new GeoEntry(13.361389, 38.115556, "Palermo"));
        var dist = await _db.GeoDistanceAsync("geodist_null", "Palermo", "NonExistent", GeoUnit.Meters);
        Assert.Null(dist);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEOPOS returns null for nonexistent member tests
    // Ref: https://redis.io/docs/latest/commands/geopos/
    //   "Returns an array where each element is either a two-element
    //    array representing the longitude and latitude, or null for
    //    a member that doesn't exist."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoPos_raw_returns_null_for_nonexistent_member()
    {
        // Ref: https://redis.io/docs/latest/commands/geopos/
        //   "Non existing elements are reported as NULL elements of the array."
        await _db.GeoAddAsync("geopos_null_raw", new GeoEntry(13.361389, 38.115556, "Palermo"));
        var result = await _db.ExecuteAsync("GEOPOS", "geopos_null_raw", "Palermo", "NonExistent");
        Assert.Equal(2, (int)result.Length);
        Assert.False(result[0].IsNull);
        Assert.True(result[1].IsNull);
    }

    [Fact]
    public async Task GeoPos_raw_nonexistent_key_returns_nulls()
    {
        // Ref: https://redis.io/docs/latest/commands/geopos/
        //   "Non existing elements are reported as NULL elements of the array."
        var result = await _db.ExecuteAsync("GEOPOS", "geopos_nokey_raw", "Member");
        Assert.Equal(1, (int)result.Length);
        Assert.True(result[0].IsNull);
    }

    [Fact]
    public async Task GeoPos_single_member_returns_null_for_nonexistent_key()
    {
        var position = await _db.GeoPositionAsync("geopos_single_nokey", "Member");
        Assert.Null(position);
    }

    [Fact]
    public async Task GeoPos_single_member_returns_null_for_nonexistent_member()
    {
        await _db.GeoAddAsync("geopos_single_null", new GeoEntry(13.361389, 38.115556, "Palermo"));
        var position = await _db.GeoPositionAsync("geopos_single_null", "NonExistent");
        Assert.Null(position);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEOADD with NX tests
    // Ref: https://redis.io/docs/latest/commands/geoadd/
    //   "XX: Only update elements that already exist. Don't add new elements."
    //   "NX: Only add new elements. Don't update already existing elements."
    //   "CH: Modify the return value from the number of new elements added,
    //    to the total number of elements changed."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoAdd_NX_only_adds_new_members()
    {
        await _db.GeoAddAsync("geoadd_nx", new GeoEntry(13.361389, 38.115556, "Palermo"));

        // Try to update Palermo and add Catania with NX
        var result = await _db.ExecuteAsync("GEOADD", "geoadd_nx", "NX",
            "1.0", "2.0", "Palermo",
            "15.087269", "37.502669", "Catania");
        var added = (long)result;
        Assert.Equal(1, added); // Only Catania was added

        // Palermo should retain original coordinates
        var pos = await _db.GeoPositionAsync("geoadd_nx", "Palermo");
        Assert.NotNull(pos);
        Assert.InRange(pos!.Value.Longitude, 13.3, 13.4);
        Assert.InRange(pos!.Value.Latitude, 38.0, 38.2);
    }

    [Fact]
    public async Task GeoAdd_NX_does_not_update_existing()
    {
        await _db.GeoAddAsync("geoadd_nx2", new GeoEntry(13.361389, 38.115556, "Palermo"));

        // Try to update position with NX — should be rejected
        var result = await _db.ExecuteAsync("GEOADD", "geoadd_nx2", "NX",
            "1.0", "2.0", "Palermo");
        Assert.Equal(0, (long)result);

        // Position unchanged
        var pos = await _db.GeoPositionAsync("geoadd_nx2", "Palermo");
        Assert.InRange(pos!.Value.Longitude, 13.3, 13.4);
    }

    // ═══════════════════════════════════════════════════════════════
    // GEOADD with XX tests
    // Ref: https://redis.io/docs/latest/commands/geoadd/
    //   "XX: Only update elements that already exist. Don't add new elements."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoAdd_XX_only_updates_existing_members()
    {
        await _db.GeoAddAsync("geoadd_xx", new GeoEntry(13.361389, 38.115556, "Palermo"));

        // Try to update Palermo and add Catania with XX
        var result = await _db.ExecuteAsync("GEOADD", "geoadd_xx", "XX",
            "1.0", "2.0", "Palermo",
            "15.087269", "37.502669", "Catania");
        Assert.Equal(0, (long)result); // XX returns 0 (no new elements added)

        // Catania should NOT exist
        var cataniaPos = await _db.GeoPositionAsync("geoadd_xx", "Catania");
        Assert.Null(cataniaPos);

        // Palermo should have updated coordinates
        var palermoPos = await _db.GeoPositionAsync("geoadd_xx", "Palermo");
        Assert.NotNull(palermoPos);
        Assert.InRange(palermoPos!.Value.Longitude, 0.9, 1.1);
        Assert.InRange(palermoPos!.Value.Latitude, 1.9, 2.1);
    }

    [Fact]
    public async Task GeoAdd_XX_does_not_add_new_members()
    {
        // Key doesn't exist yet
        var result = await _db.ExecuteAsync("GEOADD", "geoadd_xx_new", "XX",
            "13.361389", "38.115556", "Palermo");
        Assert.Equal(0, (long)result);
        Assert.False(await _db.KeyExistsAsync("geoadd_xx_new"));
    }

    // ═══════════════════════════════════════════════════════════════
    // GEOADD with CH tests
    // Ref: https://redis.io/docs/latest/commands/geoadd/
    //   "CH: Modify the return value from the number of new elements
    //    added, to the total number of elements changed (added + updated)."
    // ═══════════════════════════════════════════════════════════════

    [Fact]
    public async Task GeoAdd_CH_returns_count_of_changed()
    {
        await _db.GeoAddAsync("geoadd_ch", new GeoEntry(13.361389, 38.115556, "Palermo"));

        // Update Palermo position and add Catania — CH should count both
        var result = await _db.ExecuteAsync("GEOADD", "geoadd_ch", "CH",
            "1.0", "2.0", "Palermo",
            "15.087269", "37.502669", "Catania");
        var changed = (long)result;
        Assert.Equal(2, changed); // 1 updated (Palermo) + 1 added (Catania)
    }

    [Fact]
    public async Task GeoAdd_CH_returns_zero_when_nothing_changed()
    {
        await _db.GeoAddAsync("geoadd_ch2", new GeoEntry(13.361389, 38.115556, "Palermo"));

        // Re-add same coordinates — nothing changed
        var result = await _db.ExecuteAsync("GEOADD", "geoadd_ch2", "CH",
            "13.361389", "38.115556", "Palermo");
        Assert.Equal(0, (long)result);
    }

    [Fact]
    public async Task GeoAdd_CH_only_counts_new_when_no_update()
    {
        await _db.GeoAddAsync("geoadd_ch3", new GeoEntry(13.361389, 38.115556, "Palermo"));

        // Add Catania only — Palermo coordinates unchanged
        var result = await _db.ExecuteAsync("GEOADD", "geoadd_ch3", "CH",
            "13.361389", "38.115556", "Palermo",
            "15.087269", "37.502669", "Catania");
        // Only Catania is new, Palermo is unchanged
        Assert.Equal(1, (long)result);
    }
}
