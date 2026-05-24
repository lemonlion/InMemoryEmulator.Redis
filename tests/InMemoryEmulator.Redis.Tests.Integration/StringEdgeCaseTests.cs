using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class StringEdgeCaseTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public StringEdgeCaseTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // --- SET with NX/XX options ---

    [Fact]
    public async Task Set_NX_on_nonexistent_key_succeeds()
    {
        var result = await _db.StringSetAsync("nx_new", "value", when: When.NotExists);
        Assert.True(result);
        Assert.Equal("value", (await _db.StringGetAsync("nx_new")).ToString());
    }

    [Fact]
    public async Task Set_NX_on_existing_key_fails()
    {
        await _db.StringSetAsync("nx_exists", "original");
        var result = await _db.StringSetAsync("nx_exists", "new", when: When.NotExists);
        Assert.False(result);
        Assert.Equal("original", (await _db.StringGetAsync("nx_exists")).ToString());
    }

    [Fact]
    public async Task Set_XX_on_existing_key_succeeds()
    {
        await _db.StringSetAsync("xx_exists", "original");
        var result = await _db.StringSetAsync("xx_exists", "updated", when: When.Exists);
        Assert.True(result);
        Assert.Equal("updated", (await _db.StringGetAsync("xx_exists")).ToString());
    }

    [Fact]
    public async Task Set_XX_on_nonexistent_key_fails()
    {
        var result = await _db.StringSetAsync("xx_missing", "value", when: When.Exists);
        Assert.False(result);
        Assert.False(await _db.KeyExistsAsync("xx_missing"));
    }

    [Fact]
    public async Task Set_with_EX_sets_expiry_in_seconds()
    {
        await _db.StringSetAsync("ex_key", "value", TimeSpan.FromSeconds(100));
        var ttl = await _db.KeyTimeToLiveAsync("ex_key");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalSeconds, 90, 100);
    }

    [Fact]
    public async Task Set_with_PX_sets_expiry_in_milliseconds()
    {
        await _db.StringSetAsync("px_key", "value", TimeSpan.FromMilliseconds(5000));
        var ttl = await _db.KeyTimeToLiveAsync("px_key");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalMilliseconds, 4000, 5000);
    }

    // --- INCR/DECR edge cases ---

    [Fact]
    public async Task Incr_on_nonexistent_key_returns_one()
    {
        var result = await _db.StringIncrementAsync("incr_new");
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task Decr_on_nonexistent_key_returns_negative_one()
    {
        var result = await _db.StringDecrementAsync("decr_new");
        Assert.Equal(-1, result);
    }

    [Fact]
    public async Task Incr_on_non_integer_string_throws()
    {
        await _db.StringSetAsync("not_int", "hello");
        await Assert.ThrowsAsync<RedisServerException>(
            async () => await _db.StringIncrementAsync("not_int"));
    }

    [Fact]
    public async Task IncrBy_with_negative_value_decrements()
    {
        await _db.StringSetAsync("incrby_neg", "10");
        var result = await _db.StringIncrementAsync("incrby_neg", -3);
        Assert.Equal(7, result);
    }

    [Fact]
    public async Task IncrBy_zero_returns_unchanged()
    {
        await _db.StringSetAsync("incrby_zero", "42");
        var result = await _db.StringIncrementAsync("incrby_zero", 0);
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task IncrByFloat_on_integer_returns_float()
    {
        await _db.StringSetAsync("incrf_int", "10");
        var result = await _db.StringIncrementAsync("incrf_int", 1.5);
        Assert.Equal(11.5, result);
    }

    [Fact]
    public async Task Decr_on_zero_returns_negative_one()
    {
        await _db.StringSetAsync("decr_zero", "0");
        var result = await _db.StringDecrementAsync("decr_zero");
        Assert.Equal(-1, result);
    }

    // --- GETRANGE edge cases ---

    [Fact]
    public async Task GetRange_with_negative_indices()
    {
        await _db.StringSetAsync("range_key", "Hello, World!");
        var result = await _db.StringGetRangeAsync("range_key", -6, -1);
        Assert.Equal("World!", result.ToString());
    }

    [Fact]
    public async Task GetRange_beyond_string_length_returns_partial()
    {
        await _db.StringSetAsync("range_short", "Hi");
        var result = await _db.StringGetRangeAsync("range_short", 0, 100);
        Assert.Equal("Hi", result.ToString());
    }

    [Fact]
    public async Task GetRange_start_greater_than_end_returns_empty()
    {
        await _db.StringSetAsync("range_inv", "Hello");
        var result = await _db.StringGetRangeAsync("range_inv", 3, 1);
        Assert.Equal("", result.ToString());
    }

    [Fact]
    public async Task GetRange_on_nonexistent_key_returns_empty()
    {
        var result = await _db.StringGetRangeAsync("range_missing", 0, 10);
        Assert.Equal("", result.ToString());
    }

    // --- SETRANGE edge cases ---

    [Fact]
    public async Task SetRange_extends_string_with_zero_bytes()
    {
        await _db.StringSetAsync("setrange_ext", "Hello");
        await _db.StringSetRangeAsync("setrange_ext", 10, "X");
        var result = await _db.StringGetAsync("setrange_ext");
        Assert.Equal(11, ((byte[])result!).Length);
    }

    [Fact]
    public async Task SetRange_on_nonexistent_key_creates_with_padding()
    {
        await _db.StringSetRangeAsync("setrange_new", 5, "hi");
        var result = await _db.StringGetAsync("setrange_new");
        Assert.Equal(7, ((byte[])result!).Length);
    }

    // --- Empty and special values ---

    [Fact]
    public async Task Set_and_Get_empty_string()
    {
        await _db.StringSetAsync("empty_str", "");
        var result = await _db.StringGetAsync("empty_str");
        Assert.Equal("", result.ToString());
        Assert.False(result.IsNull);
    }

    [Fact]
    public async Task Strlen_on_nonexistent_returns_zero()
    {
        var len = await _db.StringLengthAsync("strlen_missing");
        Assert.Equal(0, len);
    }

    [Fact]
    public async Task Strlen_on_empty_string_returns_zero()
    {
        await _db.StringSetAsync("strlen_empty", "");
        var len = await _db.StringLengthAsync("strlen_empty");
        Assert.Equal(0, len);
    }

    [Fact]
    public async Task Append_to_nonexistent_creates_key()
    {
        var len = await _db.StringAppendAsync("append_new", "hello");
        Assert.Equal(5, len);
        Assert.Equal("hello", (await _db.StringGetAsync("append_new")).ToString());
    }

    // --- MGET/MSET edge cases ---

    [Fact]
    public async Task MGet_with_mix_of_existing_and_nonexistent()
    {
        await _db.StringSetAsync("mget_a", "value_a");
        await _db.StringSetAsync("mget_c", "value_c");

        var results = await _db.StringGetAsync(new RedisKey[] { "mget_a", "mget_b", "mget_c" });
        Assert.Equal("value_a", results[0].ToString());
        Assert.True(results[1].IsNull);
        Assert.Equal("value_c", results[2].ToString());
    }

    [Fact]
    public async Task MSet_overwrites_existing_keys()
    {
        await _db.StringSetAsync("mset_x", "old");
        await _db.StringSetAsync(new KeyValuePair<RedisKey, RedisValue>[]
        {
            new("mset_x", "new"),
            new("mset_y", "val_y")
        });
        Assert.Equal("new", (await _db.StringGetAsync("mset_x")).ToString());
        Assert.Equal("val_y", (await _db.StringGetAsync("mset_y")).ToString());
    }

    // --- Large value handling ---

    [Fact]
    public async Task Set_and_Get_large_value()
    {
        var largeValue = new string('X', 100_000);
        await _db.StringSetAsync("large_key", largeValue);
        var result = await _db.StringGetAsync("large_key");
        Assert.Equal(largeValue, result.ToString());
    }

    [Fact]
    public async Task Strlen_on_large_value()
    {
        var largeValue = new string('A', 50_000);
        await _db.StringSetAsync("large_strlen", largeValue);
        var len = await _db.StringLengthAsync("large_strlen");
        Assert.Equal(50_000, len);
    }
}
