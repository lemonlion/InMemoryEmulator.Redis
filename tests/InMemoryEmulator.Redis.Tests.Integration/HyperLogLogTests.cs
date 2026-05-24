using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class HyperLogLogTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public HyperLogLogTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task PfAdd_and_PfCount_tracks_cardinality()
    {
        await _db.HyperLogLogAddAsync("hll1", new RedisValue[] { "a", "b", "c", "d" });
        var count = await _db.HyperLogLogLengthAsync("hll1");
        // HLL is probabilistic, but for 4 elements it should be close
        Assert.InRange(count, 3, 6);
    }

    [Fact]
    public async Task PfAdd_duplicates_dont_increase_count()
    {
        await _db.HyperLogLogAddAsync("hll2", new RedisValue[] { "x", "y", "z" });
        var count1 = await _db.HyperLogLogLengthAsync("hll2");

        await _db.HyperLogLogAddAsync("hll2", new RedisValue[] { "x", "y", "z" });
        var count2 = await _db.HyperLogLogLengthAsync("hll2");

        Assert.Equal(count1, count2);
    }

    [Fact]
    public async Task PfMerge_combines_sets()
    {
        await _db.HyperLogLogAddAsync("hll_a", new RedisValue[] { "1", "2", "3" });
        await _db.HyperLogLogAddAsync("hll_b", new RedisValue[] { "4", "5", "6" });

        await _db.HyperLogLogMergeAsync("hll_merged", new RedisKey[] { "hll_a", "hll_b" });
        var count = await _db.HyperLogLogLengthAsync("hll_merged");
        Assert.InRange(count, 4, 8);
    }

    [Fact]
    public async Task PfCount_on_nonexistent_returns_zero()
    {
        var count = await _db.HyperLogLogLengthAsync("hll_nonexistent");
        Assert.Equal(0, count);
    }
}
