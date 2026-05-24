using InMemoryEmulator.Redis.Tests.Infrastructure;
using StackExchange.Redis;
using Xunit;

namespace InMemoryEmulator.Redis.Tests.Integration;

[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class FaultInjectionTests
{
    [Fact]
    public async Task FaultInjector_can_simulate_errors()
    {
        await using var redis = await InMemoryRedis.CreateAsync(b =>
            b.WithFaultInjector((cmd, _) => cmd == "GET" ? "simulated failure" : null));

        var db = redis.Database;
        await db.StringSetAsync("key", "value");

        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => { await db.StringGetAsync("key"); });
        Assert.Contains("simulated failure", ex.Message);
    }

    [Fact]
    public async Task FaultInjector_can_be_selective()
    {
        await using var redis = await InMemoryRedis.CreateAsync(b =>
            b.WithFaultInjector((cmd, args) =>
            {
                if (cmd == "GET" && args?.Length > 0 && args[0] == "blocked_key")
                    return "access denied";
                return null;
            }));

        var db = redis.Database;
        await db.StringSetAsync("allowed_key", "ok");
        await db.StringSetAsync("blocked_key", "secret");

        var allowed = await db.StringGetAsync("allowed_key");
        Assert.Equal("ok", allowed.ToString());

        var ex = await Assert.ThrowsAsync<RedisServerException>(
            async () => { await db.StringGetAsync("blocked_key"); });
        Assert.Contains("access denied", ex.Message);
    }

    [Fact]
    public async Task CommandLog_records_all_commands()
    {
        await using var redis = await InMemoryRedis.CreateAsync();
        var db = redis.Database;

        await db.PingAsync();
        redis.CommandLog.Clear();

        await db.StringSetAsync("log_key", "value");
        await db.StringGetAsync("log_key");
        await db.KeyDeleteAsync("log_key");

        var setCmds = redis.CommandLog.GetByCommand("SET");
        var getCmds = redis.CommandLog.GetByCommand("GET");
        // StackExchange.Redis uses UNLINK by default for KeyDeleteAsync
        var delCmds = redis.CommandLog.GetByCommand("UNLINK");
        if (delCmds.Count == 0) delCmds = redis.CommandLog.GetByCommand("DEL");

        Assert.True(setCmds.Count >= 1, $"Expected SET commands. All commands: {string.Join(", ", redis.CommandLog.GetAll().Select(c => c.CommandName))}");
        Assert.True(getCmds.Count >= 1, "Expected GET commands");
        Assert.True(delCmds.Count >= 1, "Expected DEL/UNLINK commands");
    }
}
