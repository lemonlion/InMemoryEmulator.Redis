using System.Text;
using InMemoryEmulator.Redis.Server;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Commands;

internal sealed class PubSubCommands : ICommandHandler
{
    private readonly PubSubBroker _broker;

    public PubSubCommands(PubSubBroker broker) => _broker = broker;

    public ValueTask<RespValue> ExecuteAsync(CommandContext context)
    {
        return context.CommandName switch
        {
            "SUBSCRIBE" => Subscribe(context),
            "UNSUBSCRIBE" => Unsubscribe(context),
            "PSUBSCRIBE" => PSubscribe(context),
            "PUNSUBSCRIBE" => PUnsubscribe(context),
            "PUBLISH" => Publish(context),
            "PUBSUB" => PubSub(context),
            "SSUBSCRIBE" => SSubscribe(context),
            "SUNSUBSCRIBE" => SUnsubscribe(context),
            "SPUBLISH" => SPublish(context),
            _ => ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown command '{context.CommandName}'"))
        };
    }

    private ValueTask<RespValue> Subscribe(CommandContext ctx)
    {
        // Ref: https://redis.io/docs/latest/commands/subscribe/
        // Redis responds with an array for EACH channel: ["subscribe", channel, count]
        // For a single channel, we return the confirmation directly
        for (int i = 0; i < ctx.Arguments.Length; i++)
        {
            var channel = ctx.GetArgString(i);
            _broker.Subscribe(ctx.Client, channel);
        }
        // Return last subscription confirmation as the response
        var lastChannel = ctx.GetArgString(ctx.Arguments.Length - 1);
        var total = ctx.Client.ChannelSubscriptions.Count + ctx.Client.PatternSubscriptions.Count;
        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString("subscribe"),
            RespValue.FromBulkString(lastChannel),
            new RespValue.Integer(total)
        }));
    }

    private ValueTask<RespValue> Unsubscribe(CommandContext ctx)
    {
        if (ctx.Arguments.Length == 0)
        {
            foreach (var ch in ctx.Client.ChannelSubscriptions.ToArray())
                _broker.Unsubscribe(ctx.Client, ch);
        }
        else
        {
            for (int i = 0; i < ctx.Arguments.Length; i++)
                _broker.Unsubscribe(ctx.Client, ctx.GetArgString(i));
        }
        var total = ctx.Client.ChannelSubscriptions.Count + ctx.Client.PatternSubscriptions.Count;
        var channel = ctx.Arguments.Length > 0 ? ctx.GetArgString(0) : "";
        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString("unsubscribe"),
            RespValue.FromBulkString(channel),
            new RespValue.Integer(total)
        }));
    }

    private ValueTask<RespValue> PSubscribe(CommandContext ctx)
    {
        for (int i = 0; i < ctx.Arguments.Length; i++)
            _broker.PSubscribe(ctx.Client, ctx.GetArgString(i));
        var lastPattern = ctx.GetArgString(ctx.Arguments.Length - 1);
        var total = ctx.Client.ChannelSubscriptions.Count + ctx.Client.PatternSubscriptions.Count;
        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString("psubscribe"),
            RespValue.FromBulkString(lastPattern),
            new RespValue.Integer(total)
        }));
    }

    private ValueTask<RespValue> PUnsubscribe(CommandContext ctx)
    {
        if (ctx.Arguments.Length == 0)
        {
            foreach (var pat in ctx.Client.PatternSubscriptions.ToArray())
                _broker.PUnsubscribe(ctx.Client, pat);
        }
        else
        {
            for (int i = 0; i < ctx.Arguments.Length; i++)
                _broker.PUnsubscribe(ctx.Client, ctx.GetArgString(i));
        }
        var total = ctx.Client.ChannelSubscriptions.Count + ctx.Client.PatternSubscriptions.Count;
        var pattern = ctx.Arguments.Length > 0 ? ctx.GetArgString(0) : "";
        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString("punsubscribe"),
            RespValue.FromBulkString(pattern),
            new RespValue.Integer(total)
        }));
    }

    private ValueTask<RespValue> Publish(CommandContext ctx)
    {
        var channel = ctx.GetArgString(0);
        var message = ctx.GetArgBytes(1) ?? Array.Empty<byte>();
        var count = _broker.Publish(channel, message);
        return ValueTask.FromResult<RespValue>(new RespValue.Integer(count));
    }

    private ValueTask<RespValue> PubSub(CommandContext ctx)
    {
        if (ctx.Arguments.Length == 0)
            return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", "wrong number of arguments for 'pubsub' command"));

        var sub = ctx.GetArgString(0).ToUpperInvariant();
        switch (sub)
        {
            case "CHANNELS":
                var pattern = ctx.Arguments.Length > 1 ? ctx.GetArgString(1) : "*";
                var channels = _broker.GetActiveChannels(pattern);
                return ValueTask.FromResult<RespValue>(new RespValue.Array(
                    channels.Select(c => (RespValue)RespValue.FromBulkString(c)).ToArray()));
            case "NUMSUB":
                var results = new List<RespValue>();
                for (int i = 1; i < ctx.Arguments.Length; i++)
                {
                    var ch = ctx.GetArgString(i);
                    results.Add(RespValue.FromBulkString(ch));
                    results.Add(new RespValue.Integer(_broker.GetSubscriberCount(ch)));
                }
                return ValueTask.FromResult<RespValue>(new RespValue.Array(results.ToArray()));
            case "NUMPAT":
                return ValueTask.FromResult<RespValue>(new RespValue.Integer(_broker.GetPatternCount()));
            case "SHARDCHANNELS":
                var shardPattern = ctx.Arguments.Length > 1 ? ctx.GetArgString(1) : "*";
                var shardChannels = _broker.GetActiveChannels(shardPattern);
                return ValueTask.FromResult<RespValue>(new RespValue.Array(
                    shardChannels.Select(c => (RespValue)RespValue.FromBulkString(c)).ToArray()));
            case "SHARDNUMSUB":
                var shardResults = new List<RespValue>();
                for (int i = 1; i < ctx.Arguments.Length; i++)
                {
                    var ch2 = ctx.GetArgString(i);
                    shardResults.Add(RespValue.FromBulkString(ch2));
                    shardResults.Add(new RespValue.Integer(_broker.GetSubscriberCount(ch2)));
                }
                return ValueTask.FromResult<RespValue>(new RespValue.Array(shardResults.ToArray()));
            default:
                return ValueTask.FromResult<RespValue>(new RespValue.Error("ERR", $"unknown subcommand '{sub}'"));
        }
    }

    // Ref: https://redis.io/docs/latest/commands/ssubscribe/
    // In standalone mode, sharded pub/sub works the same as regular pub/sub
    private ValueTask<RespValue> SSubscribe(CommandContext ctx)
    {
        for (int i = 0; i < ctx.Arguments.Length; i++)
        {
            var channel = ctx.GetArgString(i);
            _broker.Subscribe(ctx.Client, channel);
        }
        var lastChannel = ctx.GetArgString(ctx.Arguments.Length - 1);
        var total = ctx.Client.ChannelSubscriptions.Count + ctx.Client.PatternSubscriptions.Count;
        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString("ssubscribe"),
            RespValue.FromBulkString(lastChannel),
            new RespValue.Integer(total)
        }));
    }

    // Ref: https://redis.io/docs/latest/commands/spublish/
    //   "Posts a message to the given shard channel."
    //   In standalone mode, SPUBLISH always returns 0 — sharded pub/sub only works in cluster mode.
    private ValueTask<RespValue> SPublish(CommandContext ctx)
    {
        return ValueTask.FromResult(RespValue.Zero);
    }

    private ValueTask<RespValue> SUnsubscribe(CommandContext ctx)
    {
        if (ctx.Arguments.Length == 0)
        {
            foreach (var ch in ctx.Client.ChannelSubscriptions.ToArray())
                _broker.Unsubscribe(ctx.Client, ch);
        }
        else
        {
            for (int i = 0; i < ctx.Arguments.Length; i++)
                _broker.Unsubscribe(ctx.Client, ctx.GetArgString(i));
        }
        var total = ctx.Client.ChannelSubscriptions.Count + ctx.Client.PatternSubscriptions.Count;
        var channel = ctx.Arguments.Length > 0 ? ctx.GetArgString(0) : "";
        return ValueTask.FromResult<RespValue>(new RespValue.Array(new RespValue[]
        {
            RespValue.FromBulkString("sunsubscribe"),
            RespValue.FromBulkString(channel),
            new RespValue.Integer(total)
        }));
    }
}
