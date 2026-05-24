namespace InMemoryEmulator.Redis.Server;

internal interface ICommandHandler
{
    ValueTask<RespValue> ExecuteAsync(CommandContext context);
}
