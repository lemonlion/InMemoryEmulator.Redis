using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class TransactionTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public TransactionTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Transaction_executes_multiple_commands_atomically()
    {
        var tran = _db.CreateTransaction();
        _ = tran.StringSetAsync("tx_key1", "value1");
        _ = tran.StringSetAsync("tx_key2", "value2");
        var committed = await tran.ExecuteAsync();

        Assert.True(committed);
        Assert.Equal("value1", (await _db.StringGetAsync("tx_key1")).ToString());
        Assert.Equal("value2", (await _db.StringGetAsync("tx_key2")).ToString());
    }

    [Fact]
    public async Task Transaction_with_condition_succeeds_when_met()
    {
        await _db.StringSetAsync("cond_key", "exists");

        var tran = _db.CreateTransaction();
        tran.AddCondition(Condition.KeyExists("cond_key"));
        _ = tran.StringSetAsync("cond_key", "updated");
        var committed = await tran.ExecuteAsync();

        Assert.True(committed);
        Assert.Equal("updated", (await _db.StringGetAsync("cond_key")).ToString());
    }

    [Fact]
    public async Task Transaction_with_condition_fails_when_not_met()
    {
        var tran = _db.CreateTransaction();
        tran.AddCondition(Condition.KeyExists("nonexistent_key"));
        _ = tran.StringSetAsync("should_not_exist", "value");
        var committed = await tran.ExecuteAsync();

        Assert.False(committed);
        Assert.False(await _db.KeyExistsAsync("should_not_exist"));
    }

    [Fact]
    public async Task Transaction_incr_returns_correct_value()
    {
        await _db.StringSetAsync("tx_counter", "10");

        var tran = _db.CreateTransaction();
        var incrTask = tran.StringIncrementAsync("tx_counter", 5);
        var committed = await tran.ExecuteAsync();

        Assert.True(committed);
        Assert.Equal(15, await incrTask);
    }
}
