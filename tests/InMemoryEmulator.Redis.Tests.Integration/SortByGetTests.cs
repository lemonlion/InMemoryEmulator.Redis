using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class SortByGetTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public SortByGetTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    // Ref: https://redis.io/docs/latest/commands/sort/
    //   "BY pattern: Use an external key to sort instead of comparing the actual elements."

    [Fact]
    public async Task Sort_BY_external_string_key()
    {
        await _db.ListRightPushAsync("mylist", new RedisValue[] { "1", "2", "3" });
        await _db.StringSetAsync("weight_1", "30");
        await _db.StringSetAsync("weight_2", "10");
        await _db.StringSetAsync("weight_3", "20");

        var sorted = await _db.SortAsync("mylist", by: "weight_*");
        Assert.Equal(3, sorted.Length);
        Assert.Equal("2", sorted[0].ToString());
        Assert.Equal("3", sorted[1].ToString());
        Assert.Equal("1", sorted[2].ToString());
    }

    // Ref: https://redis.io/docs/latest/commands/sort/
    //   "GET pattern: retrieve external key values for each sorted element"

    [Fact]
    public async Task Sort_GET_external_string_key()
    {
        await _db.ListRightPushAsync("mylist2", new RedisValue[] { "1", "2", "3" });
        await _db.StringSetAsync("name_1", "Alice");
        await _db.StringSetAsync("name_2", "Bob");
        await _db.StringSetAsync("name_3", "Charlie");

        var sorted = await _db.SortAsync("mylist2", get: new RedisValue[] { "name_*" });
        Assert.Equal(3, sorted.Length);
        Assert.Equal("Alice", sorted[0].ToString());
        Assert.Equal("Bob", sorted[1].ToString());
        Assert.Equal("Charlie", sorted[2].ToString());
    }

    // Ref: https://redis.io/docs/latest/commands/sort/
    //   "GET #: returns the element itself"

    [Fact]
    public async Task Sort_GET_hash_returns_element_itself()
    {
        await _db.ListRightPushAsync("mylist3", new RedisValue[] { "3", "1", "2" });

        var sorted = await _db.SortAsync("mylist3", get: new RedisValue[] { "#" });
        Assert.Equal(3, sorted.Length);
        Assert.Equal("1", sorted[0].ToString());
        Assert.Equal("2", sorted[1].ToString());
        Assert.Equal("3", sorted[2].ToString());
    }

    [Fact]
    public async Task Sort_BY_and_GET_combined()
    {
        await _db.ListRightPushAsync("users", new RedisValue[] { "1", "2", "3" });
        await _db.StringSetAsync("weight_1", "30");
        await _db.StringSetAsync("weight_2", "10");
        await _db.StringSetAsync("weight_3", "20");
        await _db.StringSetAsync("name_1", "Alice");
        await _db.StringSetAsync("name_2", "Bob");
        await _db.StringSetAsync("name_3", "Charlie");

        var sorted = await _db.SortAsync("users", by: "weight_*", get: new RedisValue[] { "name_*" });
        Assert.Equal(3, sorted.Length);
        Assert.Equal("Bob", sorted[0].ToString());
        Assert.Equal("Charlie", sorted[1].ToString());
        Assert.Equal("Alice", sorted[2].ToString());
    }

    // Ref: https://redis.io/docs/latest/commands/sort/
    //   "BY nosort: skip sorting, preserve insertion order"

    [Fact]
    public async Task Sort_BY_nosort_preserves_insertion_order()
    {
        await _db.ListRightPushAsync("nosort", new RedisValue[] { "c", "a", "b" });

        var sorted = await _db.SortAsync("nosort", by: "nosort");
        Assert.Equal(3, sorted.Length);
        Assert.Equal("c", sorted[0].ToString());
        Assert.Equal("a", sorted[1].ToString());
        Assert.Equal("b", sorted[2].ToString());
    }

    [Fact]
    public async Task Sort_BY_nosort_with_GET()
    {
        await _db.ListRightPushAsync("nosort2", new RedisValue[] { "2", "1", "3" });
        await _db.StringSetAsync("name_1", "Alice");
        await _db.StringSetAsync("name_2", "Bob");
        await _db.StringSetAsync("name_3", "Charlie");

        var sorted = await _db.SortAsync("nosort2", by: "nosort", get: new RedisValue[] { "name_*" });
        Assert.Equal(3, sorted.Length);
        Assert.Equal("Bob", sorted[0].ToString());
        Assert.Equal("Alice", sorted[1].ToString());
        Assert.Equal("Charlie", sorted[2].ToString());
    }

    // Ref: https://redis.io/docs/latest/commands/sort/
    //   "If the pattern contains ->, the rest is treated as a hash field."

    [Fact]
    public async Task Sort_BY_hash_field()
    {
        await _db.ListRightPushAsync("hashsort", new RedisValue[] { "1", "2", "3" });
        await _db.HashSetAsync("obj_1", "weight", "30");
        await _db.HashSetAsync("obj_2", "weight", "10");
        await _db.HashSetAsync("obj_3", "weight", "20");

        var sorted = await _db.SortAsync("hashsort", by: "obj_*->weight");
        Assert.Equal(3, sorted.Length);
        Assert.Equal("2", sorted[0].ToString());
        Assert.Equal("3", sorted[1].ToString());
        Assert.Equal("1", sorted[2].ToString());
    }

    [Fact]
    public async Task Sort_GET_hash_field()
    {
        await _db.ListRightPushAsync("hashget", new RedisValue[] { "3", "1", "2" });
        await _db.HashSetAsync("user_1", "name", "Alice");
        await _db.HashSetAsync("user_2", "name", "Bob");
        await _db.HashSetAsync("user_3", "name", "Charlie");

        var sorted = await _db.SortAsync("hashget", get: new RedisValue[] { "user_*->name" });
        Assert.Equal(3, sorted.Length);
        Assert.Equal("Alice", sorted[0].ToString());
        Assert.Equal("Bob", sorted[1].ToString());
        Assert.Equal("Charlie", sorted[2].ToString());
    }

    [Fact]
    public async Task Sort_BY_hash_field_with_GET_hash_field()
    {
        await _db.ListRightPushAsync("combo", new RedisValue[] { "1", "2", "3" });
        await _db.HashSetAsync("item_1", new HashEntry[] { new("weight", "30"), new("name", "Alpha") });
        await _db.HashSetAsync("item_2", new HashEntry[] { new("weight", "10"), new("name", "Beta") });
        await _db.HashSetAsync("item_3", new HashEntry[] { new("weight", "20"), new("name", "Gamma") });

        var sorted = await _db.SortAsync("combo", by: "item_*->weight", get: new RedisValue[] { "item_*->name" });
        Assert.Equal(3, sorted.Length);
        Assert.Equal("Beta", sorted[0].ToString());
        Assert.Equal("Gamma", sorted[1].ToString());
        Assert.Equal("Alpha", sorted[2].ToString());
    }

    [Fact]
    public async Task Sort_GET_missing_key_returns_nil()
    {
        await _db.ListRightPushAsync("nilget", new RedisValue[] { "1", "2" });
        await _db.StringSetAsync("name_1", "Alice");
        // name_2 does not exist

        var sorted = await _db.SortAsync("nilget", get: new RedisValue[] { "name_*" });
        Assert.Equal(2, sorted.Length);
        Assert.Equal("Alice", sorted[0].ToString());
        Assert.True(sorted[1].IsNull);
    }

    [Fact]
    public async Task Sort_BY_with_DESC()
    {
        await _db.ListRightPushAsync("bydesc", new RedisValue[] { "1", "2", "3" });
        await _db.StringSetAsync("weight_1", "30");
        await _db.StringSetAsync("weight_2", "10");
        await _db.StringSetAsync("weight_3", "20");

        var sorted = await _db.SortAsync("bydesc", by: "weight_*", order: Order.Descending);
        Assert.Equal(3, sorted.Length);
        Assert.Equal("1", sorted[0].ToString());
        Assert.Equal("3", sorted[1].ToString());
        Assert.Equal("2", sorted[2].ToString());
    }

    [Fact]
    public async Task Sort_BY_with_ALPHA()
    {
        await _db.ListRightPushAsync("byalpha", new RedisValue[] { "1", "2", "3" });
        await _db.StringSetAsync("name_1", "Charlie");
        await _db.StringSetAsync("name_2", "Alice");
        await _db.StringSetAsync("name_3", "Bob");

        var sorted = await _db.SortAsync("byalpha", by: "name_*", sortType: SortType.Alphabetic);
        Assert.Equal(3, sorted.Length);
        Assert.Equal("2", sorted[0].ToString());
        Assert.Equal("3", sorted[1].ToString());
        Assert.Equal("1", sorted[2].ToString());
    }

    [Fact]
    public async Task Sort_multiple_GET_patterns()
    {
        await _db.ListRightPushAsync("multi", new RedisValue[] { "2", "1" });
        await _db.StringSetAsync("name_1", "Alice");
        await _db.StringSetAsync("name_2", "Bob");
        await _db.StringSetAsync("age_1", "30");
        await _db.StringSetAsync("age_2", "25");

        var sorted = await _db.SortAsync("multi", get: new RedisValue[] { "name_*", "age_*" });
        Assert.Equal(4, sorted.Length);
        Assert.Equal("Alice", sorted[0].ToString());
        Assert.Equal("30", sorted[1].ToString());
        Assert.Equal("Bob", sorted[2].ToString());
        Assert.Equal("25", sorted[3].ToString());
    }

    [Fact]
    public async Task Sort_GET_hash_and_element()
    {
        await _db.ListRightPushAsync("both", new RedisValue[] { "2", "1" });
        await _db.StringSetAsync("name_1", "Alice");
        await _db.StringSetAsync("name_2", "Bob");

        var sorted = await _db.SortAsync("both", get: new RedisValue[] { "#", "name_*" });
        Assert.Equal(4, sorted.Length);
        Assert.Equal("1", sorted[0].ToString());
        Assert.Equal("Alice", sorted[1].ToString());
        Assert.Equal("2", sorted[2].ToString());
        Assert.Equal("Bob", sorted[3].ToString());
    }

    [Fact]
    public async Task Sort_BY_with_LIMIT()
    {
        await _db.ListRightPushAsync("bylimit", new RedisValue[] { "1", "2", "3", "4", "5" });
        await _db.StringSetAsync("weight_1", "50");
        await _db.StringSetAsync("weight_2", "10");
        await _db.StringSetAsync("weight_3", "30");
        await _db.StringSetAsync("weight_4", "20");
        await _db.StringSetAsync("weight_5", "40");

        var sorted = await _db.SortAsync("bylimit", by: "weight_*", skip: 1, take: 2);
        Assert.Equal(2, sorted.Length);
        Assert.Equal("4", sorted[0].ToString());
        Assert.Equal("3", sorted[1].ToString());
    }

    [Fact]
    public async Task Sort_on_set_with_BY_and_GET()
    {
        await _db.SetAddAsync("myset", new RedisValue[] { "1", "2", "3" });
        await _db.StringSetAsync("weight_1", "30");
        await _db.StringSetAsync("weight_2", "10");
        await _db.StringSetAsync("weight_3", "20");
        await _db.StringSetAsync("label_1", "X");
        await _db.StringSetAsync("label_2", "Y");
        await _db.StringSetAsync("label_3", "Z");

        var sorted = await _db.SortAsync("myset", by: "weight_*", get: new RedisValue[] { "label_*" });
        Assert.Equal(3, sorted.Length);
        Assert.Equal("Y", sorted[0].ToString());
        Assert.Equal("Z", sorted[1].ToString());
        Assert.Equal("X", sorted[2].ToString());
    }
}
