using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace InMemoryEmulator.Redis;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection UseInMemoryRedis(
        this IServiceCollection services,
        Action<InMemoryRedisOptions>? configure = null)
    {
        var options = new InMemoryRedisOptions();
        configure?.Invoke(options);

        var muxDescriptors = services.Where(d => d.ServiceType == typeof(IConnectionMultiplexer)).ToList();
        foreach (var d in muxDescriptors) services.Remove(d);
        var dbDescriptors = services.Where(d => d.ServiceType == typeof(IDatabase)).ToList();
        foreach (var d in dbDescriptors) services.Remove(d);

        var result = InMemoryRedis.CreateAsync(b =>
        {
            if (options.Password != null) b.WithPassword(options.Password);
            if (options.SeedData != null) b.WithSeedData(options.SeedData);
        }).GetAwaiter().GetResult();

        services.AddSingleton<IConnectionMultiplexer>(result.Multiplexer);
        services.AddSingleton<IDatabase>(result.Database);
        services.AddSingleton(result);

        options.OnCreated?.Invoke(result);

        return services;
    }
}

public sealed class InMemoryRedisOptions
{
    public string? Password { get; set; }
    public Action<InMemoryRedisStore>? SeedData { get; set; }
    public Action<InMemoryRedisResult>? OnCreated { get; set; }
}
