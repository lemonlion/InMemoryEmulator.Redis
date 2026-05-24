using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class HyperLogLogCommands : ICommandHandler
{
    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "PFADD" => PfAdd(context),
            "PFCOUNT" => PfCount(context),
            "PFMERGE" => PfMerge(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private static ValueTask<RespValue> PfAdd(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/pfadd/
        var key = ctx.GetArgString(0);
        var entry = ctx.Database.GetEntry(key);
        RedisHyperLogLog hll;

        if (entry == null)
        {
            hll = new RedisHyperLogLog();
            ctx.Database.SetEntry(key, hll);
        }
        else if (entry is RedisHyperLogLog existing)
        {
            hll = existing;
        }
        else
        {
            throw new WrongTypeException();
        }

        bool changed = false;
        for (int i = 1; i < ctx.Arguments.Length; i++)
        {
            var element = ctx.GetArgBytes(i) ?? Array.Empty<byte>();
            if (hll.Add(element)) changed = true;
        }

        ctx.Database.IncrementVersion(key);
        return ValueTask.FromResult<RespValue>(changed ? RespValue.One : RespValue.Zero);
    }

    private static ValueTask<RespValue> PfCount(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/pfcount/
        if (ctx.Arguments.Length == 1)
        {
            var key = ctx.GetArgString(0);
            var entry = ctx.Database.GetEntry(key);
            if (entry == null) return ValueTask.FromResult(RespValue.Zero);
            if (entry is not RedisHyperLogLog hll) throw new WrongTypeException();
            return ValueTask.FromResult<RespValue>(new RespValue.Integer(hll.Count()));
        }

        // Multiple keys: union count
        var merged = new RedisHyperLogLog();
        for (int i = 0; i < ctx.Arguments.Length; i++)
        {
            var key = ctx.GetArgString(i);
            var entry = ctx.Database.GetEntry(key);
            if (entry == null) continue;
            if (entry is not RedisHyperLogLog hll) throw new WrongTypeException();
            merged.MergeWith(hll);
        }
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(merged.Count()));
    }

    private static ValueTask<RespValue> PfMerge(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/pfmerge/
        var destKey = ctx.GetArgString(0);
        var dest = new RedisHyperLogLog();

        for (int i = 1; i < ctx.Arguments.Length; i++)
        {
            var key = ctx.GetArgString(i);
            var entry = ctx.Database.GetEntry(key);
            if (entry == null) continue;
            if (entry is not RedisHyperLogLog hll) throw new WrongTypeException();
            dest.MergeWith(hll);
        }

        ctx.Database.SetEntry(destKey, dest);
        return ValueTask.FromResult(RespValue.Ok);
    }
}
