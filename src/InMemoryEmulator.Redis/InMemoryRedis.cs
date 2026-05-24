namespace InMemoryEmulator.Redis;

public static class InMemoryRedis
{
    public static async Task<InMemoryRedisResult> CreateAsync(
        Action<InMemoryRedisBuilder>? configure = null)
    {
        var builder = new InMemoryRedisBuilder();
        configure?.Invoke(builder);
        return await builder.BuildAsync();
    }

    public static InMemoryRedisBuilder Builder() => new();
}
