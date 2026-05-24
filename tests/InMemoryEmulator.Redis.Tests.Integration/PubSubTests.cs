using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Collection(IntegrationCollection.Name)]
[Trait(TestTraits.Target, TestTraits.All)]
public class PubSubTests : IAsyncLifetime
{
    private readonly RedisSession _session;
    private IRedisTestFixture _fixture = null!;
    private IDatabase _db = null!;

    public PubSubTests(RedisSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        _db = await _fixture.GetDatabaseAsync();
        await _fixture.FlushAllAsync();
    }

    public async ValueTask DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task Publish_to_channel_with_no_subscribers_returns_zero()
    {
        var mux = await _fixture.GetMultiplexerAsync();
        var sub = mux.GetSubscriber();
        var count = await sub.PublishAsync(RedisChannel.Literal("empty_channel"), "msg");
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Publish_returns_number_of_receivers()
    {
        var mux = await _fixture.GetMultiplexerAsync();
        var sub = mux.GetSubscriber();
        var received = new TaskCompletionSource<RedisValue>();

        var queue = await sub.SubscribeAsync(RedisChannel.Literal("pubsub_test"));
        queue.OnMessage(msg => received.TrySetResult(msg.Message));

        await Task.Delay(100);
        var count = await sub.PublishAsync(RedisChannel.Literal("pubsub_test"), "hello");

        var message = await received.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal("hello", message.ToString());
        Assert.True(count >= 1);

        await sub.UnsubscribeAsync(RedisChannel.Literal("pubsub_test"));
    }
}
