using InMemoryEmulator.Redis.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class DependencyInjectionTests
{
    [Fact]
    public void UseInMemoryRedis_registers_multiplexer_and_database()
    {
        var services = new ServiceCollection();
        services.UseInMemoryRedis();

        using var provider = services.BuildServiceProvider();
        var mux = provider.GetRequiredService<IConnectionMultiplexer>();
        var db = provider.GetRequiredService<IDatabase>();
        var result = provider.GetRequiredService<InMemoryRedisResult>();

        Assert.NotNull(mux);
        Assert.NotNull(db);
        Assert.NotNull(result);
        Assert.True(mux.IsConnected);
    }

    [Fact]
    public async Task UseInMemoryRedis_with_seed_data_populates_store()
    {
        var services = new ServiceCollection();
        services.UseInMemoryRedis(opts =>
        {
            opts.OnCreated = result =>
            {
                result.Database.StringSet("seeded", "value");
            };
        });

        using var provider = services.BuildServiceProvider();
        var db = provider.GetRequiredService<IDatabase>();

        var value = await db.StringGetAsync("seeded");
        Assert.Equal("value", value.ToString());
    }

    [Fact]
    public void UseInMemoryRedis_replaces_existing_registrations()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConnectionMultiplexer>(_ => throw new InvalidOperationException("should be replaced"));
        services.UseInMemoryRedis();

        using var provider = services.BuildServiceProvider();
        var mux = provider.GetRequiredService<IConnectionMultiplexer>();
        Assert.True(mux.IsConnected);
    }
}
