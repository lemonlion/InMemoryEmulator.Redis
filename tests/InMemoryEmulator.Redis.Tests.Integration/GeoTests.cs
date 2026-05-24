using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class GeoTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public GeoTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task GeoAdd_and_GeoPos_roundtrip()
    {
        var added = await _db.GeoAddAsync("places", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
        });

        Assert.Equal(2, added);

        var positions = await _db.GeoPositionAsync("places", new RedisValue[] { "Palermo", "Catania" });
        Assert.Equal(2, positions.Length);
        Assert.NotNull(positions[0]);
        Assert.NotNull(positions[1]);
        // Geohash encoding has some precision loss
        Assert.InRange(positions[0]!.Value.Longitude, 13.3, 13.4);
        Assert.InRange(positions[0]!.Value.Latitude, 38.0, 38.2);
    }

    [Fact]
    public async Task GeoDist_returns_distance()
    {
        await _db.GeoAddAsync("cities", new GeoEntry[]
        {
            new(13.361389, 38.115556, "Palermo"),
            new(15.087269, 37.502669, "Catania"),
        });

        var dist = await _db.GeoDistanceAsync("cities", "Palermo", "Catania", GeoUnit.Kilometers);
        Assert.NotNull(dist);
        // Real distance is ~166 km
        Assert.InRange(dist!.Value, 100, 250);
    }

    [Fact]
    public async Task GeoAdd_returns_false_for_existing_members()
    {
        await _db.GeoAddAsync("geo1", new GeoEntry(1.0, 2.0, "member1"));
        var added = await _db.GeoAddAsync("geo1", new GeoEntry(1.0, 2.0, "member1"));
        Assert.False(added);
    }
}
