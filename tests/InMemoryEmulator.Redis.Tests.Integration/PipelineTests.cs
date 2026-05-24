using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class PipelineTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public PipelineTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Batch_executes_multiple_commands()
    {
        var batch = _db.CreateBatch();
        var t1 = batch.StringSetAsync("batch1", "v1");
        var t2 = batch.StringSetAsync("batch2", "v2");
        var t3 = batch.StringSetAsync("batch3", "v3");
        batch.Execute();

        await Task.WhenAll(t1, t2, t3);

        Assert.Equal("v1", (await _db.StringGetAsync("batch1")).ToString());
        Assert.Equal("v2", (await _db.StringGetAsync("batch2")).ToString());
        Assert.Equal("v3", (await _db.StringGetAsync("batch3")).ToString());
    }

    [Fact]
    public async Task Fire_and_forget_commands_execute()
    {
        await _db.StringSetAsync("ff_key", "value", flags: CommandFlags.FireAndForget);
        await Task.Delay(50);
        var result = await _db.StringGetAsync("ff_key");
        Assert.Equal("value", result.ToString());
    }

    [Fact]
    public async Task Multiple_async_commands_in_flight()
    {
        var tasks = new List<Task>();
        for (int i = 0; i < 100; i++)
            tasks.Add(_db.StringSetAsync($"parallel_{i}", $"value_{i}"));

        await Task.WhenAll(tasks);

        for (int i = 0; i < 100; i++)
        {
            var val = await _db.StringGetAsync($"parallel_{i}");
            Assert.Equal($"value_{i}", val.ToString());
        }
    }

    [Fact]
    public async Task Mixed_data_type_operations_in_pipeline()
    {
        var batch = _db.CreateBatch();
        var t1 = batch.StringSetAsync("str_pipe", "hello");
        var t2 = batch.ListRightPushAsync("list_pipe", "item1");
        var t3 = batch.SetAddAsync("set_pipe", "member1");
        var t4 = batch.HashSetAsync("hash_pipe", "field1", "val1");
        batch.Execute();

        await Task.WhenAll(t1, t2, t3, t4);

        Assert.Equal(RedisType.String, await _db.KeyTypeAsync("str_pipe"));
        Assert.Equal(RedisType.List, await _db.KeyTypeAsync("list_pipe"));
        Assert.Equal(RedisType.Set, await _db.KeyTypeAsync("set_pipe"));
        Assert.Equal(RedisType.Hash, await _db.KeyTypeAsync("hash_pipe"));
    }
}
