namespace InMemoryEmulator.Redis.Tests.Infrastructure;

public static class TestFixtureFactory
{
    public static IRedisTestFixture Create(RedisSession session) =>
        session.Target == TestTarget.Docker
            ? new DockerTestFixture(session)
            : new InMemoryTestFixture();
}
