using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class TransactionCommands : ICommandHandler
{
    private readonly CommandRouter _router;
    private readonly InMemoryRedisStore _store;

    public TransactionCommands(CommandRouter router, InMemoryRedisStore store)
    {
        _router = router;
        _store = store;
    }

    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "MULTI" => Multi(context),
            "EXEC" => Exec(context),
            "DISCARD" => Discard(context),
            "WATCH" => Watch(context),
            "UNWATCH" => Unwatch(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static ValueTask<RespValue> Multi(CommandContext ctx)
    {
        if (ctx.Client.InTransaction)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "MULTI calls can not be nested"));
        ctx.Client.InTransaction = true;
        ctx.Client.TransactionQueue = new List<(string, RespValue[])>();
        return ValueTask.FromResult(RespValue.Ok);
    }

    private async ValueTask<RespValue> Exec(CommandContext ctx)
    {
        if (!ctx.Client.InTransaction)
            return new RespValue.Error("ERR", "EXEC without MULTI");

        // Check WATCH
        if (ctx.Client.WatchedKeyVersions != null)
        {
            var db = _store.GetDatabase(ctx.Client.SelectedDatabase);
            foreach (var (key, version) in ctx.Client.WatchedKeyVersions)
            {
                if (db.GetKeyVersion(key) != version)
                {
                    ctx.Client.InTransaction = false;
                    ctx.Client.TransactionQueue = null;
                    ctx.Client.WatchedKeyVersions = null;
                    return RespValue.NullArray;
                }
            }
        }

        var queue = ctx.Client.TransactionQueue!;
        ctx.Client.InTransaction = false;
        ctx.Client.TransactionQueue = null;
        ctx.Client.WatchedKeyVersions = null;

        var results = new RespValue[queue.Count];
        var db2 = _store.GetDatabase(ctx.Client.SelectedDatabase);

        for (int i = 0; i < queue.Count; i++)
        {
            var (cmdName, args) = queue[i];
            var cmdCtx = new CommandContext
            {
                CommandName = cmdName,
                Arguments = args,
                Client = ctx.Client,
                Database = db2,
                CancellationToken = ctx.CancellationToken
            };

            try
            {
                results[i] = await _router.ExecuteAsync(cmdCtx);
            }
            catch (WrongTypeException)
            {
                results[i] = RespValue.WrongTypeError;
            }
            catch (Exception ex)
            {
                results[i] = new RespValue.Error("ERR", ex.Message);
            }
        }

        return new RespValue.Array(results);
    }

    private static ValueTask<RespValue> Discard(CommandContext ctx)
    {
        if (!ctx.Client.InTransaction)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "DISCARD without MULTI"));
        ctx.Client.InTransaction = false;
        ctx.Client.TransactionQueue = null;
        ctx.Client.WatchedKeyVersions = null;
        return ValueTask.FromResult(RespValue.Ok);
    }

    private ValueTask<RespValue> Watch(CommandContext ctx)
    {
        if (ctx.Client.InTransaction)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "WATCH inside MULTI is not allowed"));

        ctx.Client.WatchedKeyVersions ??= new Dictionary<string, long>();
        var db = _store.GetDatabase(ctx.Client.SelectedDatabase);
        for (int i = 0; i < ctx.Arguments.Length; i++)
        {
            var key = ctx.GetArgString(i);
            ctx.Client.WatchedKeyVersions[key] = db.GetKeyVersion(key);
        }
        return ValueTask.FromResult(RespValue.Ok);
    }

    private static ValueTask<RespValue> Unwatch(CommandContext ctx)
    {
        ctx.Client.WatchedKeyVersions = null;
        return ValueTask.FromResult(RespValue.Ok);
    }
}
