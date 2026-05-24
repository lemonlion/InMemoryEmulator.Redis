using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class StringCommandTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public StringCommandTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Set_and_Get_roundtrips_string()
    {
        await _db.StringSetAsync("key1", "hello");
        var result = await _db.StringGetAsync("key1");
        Assert.Equal("hello", result.ToString());
    }

    [Fact]
    public async Task Get_nonexistent_returns_null()
    {
        var result = await _db.StringGetAsync("nonexistent");
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task Set_with_expiry_expires_key()
    {
        await _db.StringSetAsync("expkey", "value", TimeSpan.FromMilliseconds(50));
        Assert.False((await _db.StringGetAsync("expkey")).IsNull);
        await Task.Delay(100);
        Assert.True((await _db.StringGetAsync("expkey")).IsNull);
    }

    [Fact]
    public async Task Incr_increments_integer_value()
    {
        await _db.StringSetAsync("counter", "10");
        var result = await _db.StringIncrementAsync("counter");
        Assert.Equal(11, result);
    }

    [Fact]
    public async Task Incr_creates_key_if_not_exists()
    {
        var result = await _db.StringIncrementAsync("newcounter");
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task Decr_decrements_value()
    {
        await _db.StringSetAsync("counter", "10");
        var result = await _db.StringDecrementAsync("counter");
        Assert.Equal(9, result);
    }

    [Fact]
    public async Task Append_appends_to_existing()
    {
        await _db.StringSetAsync("appendkey", "hello");
        var len = await _db.StringAppendAsync("appendkey", " world");
        Assert.Equal(11, len);
        Assert.Equal("hello world", (await _db.StringGetAsync("appendkey")).ToString());
    }

    [Fact]
    public async Task Strlen_returns_length()
    {
        await _db.StringSetAsync("lenkey", "hello");
        var len = await _db.StringLengthAsync("lenkey");
        Assert.Equal(5, len);
    }

    [Fact]
    public async Task SetNx_only_sets_if_not_exists()
    {
        await _db.StringSetAsync("nxkey", "original");
        var set = await _db.StringSetAsync("nxkey", "new", when: When.NotExists);
        Assert.False(set);
        Assert.Equal("original", (await _db.StringGetAsync("nxkey")).ToString());
    }
}
