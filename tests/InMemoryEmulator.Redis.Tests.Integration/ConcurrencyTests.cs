using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class ConcurrencyTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public ConcurrencyTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Concurrent_incr_is_atomic()
    {
        await _db.StringSetAsync("conc_incr", "0");
        var tasks = Enumerable.Range(0, 50)
            .Select(_ => _db.StringIncrementAsync("conc_incr"))
            .ToArray();
        await Task.WhenAll(tasks);
        var result = await _db.StringGetAsync("conc_incr");
        Assert.Equal("50", result.ToString());
    }

    [Fact]
    public async Task Concurrent_set_operations_dont_corrupt()
    {
        var tasks = Enumerable.Range(0, 100)
            .Select(i => _db.StringSetAsync($"conc_set_{i}", $"value_{i}"))
            .ToArray();
        await Task.WhenAll(tasks);

        for (int i = 0; i < 100; i++)
        {
            var val = await _db.StringGetAsync($"conc_set_{i}");
            Assert.Equal($"value_{i}", val.ToString());
        }
    }

    [Fact]
    public async Task Concurrent_lpush_maintains_integrity()
    {
        var tasks = Enumerable.Range(0, 50)
            .Select(i => _db.ListRightPushAsync("conc_list", $"item_{i}"))
            .ToArray();
        await Task.WhenAll(tasks);
        var len = await _db.ListLengthAsync("conc_list");
        Assert.Equal(50, len);
    }

    [Fact]
    public async Task Concurrent_sadd_maintains_cardinality()
    {
        var tasks = Enumerable.Range(0, 50)
            .Select(i => _db.SetAddAsync("conc_set", $"member_{i}"))
            .ToArray();
        await Task.WhenAll(tasks);
        var count = await _db.SetLengthAsync("conc_set");
        Assert.Equal(50, count);
    }

    [Fact]
    public async Task Concurrent_reads_and_writes_dont_crash()
    {
        await _db.StringSetAsync("conc_rw", "initial");
        var tasks = new List<Task>();
        for (int i = 0; i < 50; i++)
        {
            tasks.Add(Task.Run(async () => { await _db.StringGetAsync("conc_rw"); }));
            tasks.Add(Task.Run(async () => { await _db.StringSetAsync("conc_rw", $"value_{i}"); }));
        }
        await Task.WhenAll(tasks);
        var final = await _db.StringGetAsync("conc_rw");
        Assert.False(final.IsNull);
    }

    [Fact]
    public async Task Batch_with_mixed_operations()
    {
        var batch = _db.CreateBatch();
        var tasks = new List<Task>();
        for (int i = 0; i < 20; i++)
        {
            tasks.Add(batch.StringSetAsync($"batch_s_{i}", $"v{i}"));
            tasks.Add(batch.ListRightPushAsync("batch_list", $"item{i}"));
            tasks.Add(batch.SetAddAsync("batch_set", $"m{i}"));
        }
        batch.Execute();
        await Task.WhenAll(tasks);

        Assert.Equal(20, await _db.ListLengthAsync("batch_list"));
        Assert.Equal(20, await _db.SetLengthAsync("batch_set"));
    }
}
