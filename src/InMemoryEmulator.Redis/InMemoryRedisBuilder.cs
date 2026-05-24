using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;
using StackExchange.Redis;

namespace InMemoryEmulator.Redis;

public sealed class InMemoryRedisBuilder
{
    private string? _password;
    private Func<string, string[]?, string?>? _faultInjector;
    private Action<InMemoryRedisStore>? _seedData;
    private Action<ConfigurationOptions>? _configureConnection;

    public InMemoryRedisBuilder WithPassword(string password) { _password = password; return this; }
    /// <summary>
    /// Set a fault injector. Return an error message string to inject an error, or null to pass through.
    /// Parameters: (commandName, arguments) => errorMessage or null.
    /// </summary>
    public InMemoryRedisBuilder WithFaultInjector(Func<string, string[]?, string?>? injector) { _faultInjector = injector; return this; }
    public InMemoryRedisBuilder WithSeedData(Action<InMemoryRedisStore> seed) { _seedData = seed; return this; }
    public InMemoryRedisBuilder ConfigureConnection(Action<ConfigurationOptions> configure) { _configureConnection = configure; return this; }

    public async Task<InMemoryRedisResult> BuildAsync()
    {
        var store = new InMemoryRedisStore();
        _seedData?.Invoke(store);

        var commandLog = new CommandLog();
        var server = new FakeRedisServer(store, commandLog, _password);
        if (_faultInjector != null)
        {
            var injector = _faultInjector;
            server.FaultInjector = (cmd, args) =>
            {
                var strArgs = args?.Select(a => a switch
                {
                    RespValue.BulkString { Data: { } data } => System.Text.Encoding.UTF8.GetString(data),
                    _ => ""
                }).ToArray();
                var error = injector(cmd, strArgs);
                if (error != null) return new RespValue.Error("ERR", error);
                return null;
            };
        }
        await server.StartAsync();

        var options = new ConfigurationOptions
        {
            EndPoints = { { server.Host, server.Port } },
            AllowAdmin = true,
            AbortOnConnectFail = false,
            ConnectTimeout = 5000,
            SyncTimeout = 5000,
            AsyncTimeout = 5000
        };
        if (_password != null) options.Password = _password;
        _configureConnection?.Invoke(options);

        var multiplexer = await ConnectionMultiplexer.ConnectAsync(options);

        return new InMemoryRedisResult(multiplexer, store, server, commandLog);
    }
}
