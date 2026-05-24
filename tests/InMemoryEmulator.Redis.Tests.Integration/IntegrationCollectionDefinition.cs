using InMemoryEmulator.Redis.Tests.Infrastructure;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[CollectionDefinition(IntegrationCollection.Name)]
public class IntegrationCollectionDefinition : ICollectionFixture<RedisSession>
{
}
