using Testcontainers.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Infrastructure;

public sealed class RedisSession : IAsyncLifetime
{
    private RedisContainer? _container;

    public TestTarget Target { get; private set; }
    public string? DockerConnectionString { get; private set; }

    public async ValueTask InitializeAsync()
    {
        var targetEnv = Environment.GetEnvironmentVariable("REDIS_TEST_TARGET");
        Target = targetEnv?.Equals("docker", StringComparison.OrdinalIgnoreCase) == true
            ? TestTarget.Docker
            : TestTarget.InMemory;

        if (Target == TestTarget.Docker)
        {
            _container = new RedisBuilder()
                .WithImage("redis:latest")
                .Build();
            await _container.StartAsync();
            DockerConnectionString = _container.GetConnectionString();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_container != null)
            await _container.DisposeAsync();
    }
}
