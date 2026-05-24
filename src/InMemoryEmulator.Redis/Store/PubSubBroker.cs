using System.Collections.Concurrent;
using System.Text;
using InMemoryEmulator.Redis.Commands;
using InMemoryEmulator.Redis.Server;

namespace InMemoryEmulator.Redis.Store;

internal sealed class PubSubBroker
{
    private readonly ConcurrentDictionary<string, ConcurrentBag<ClientConnection>> _channelSubs = new();
    private readonly ConcurrentDictionary<string, ConcurrentBag<ClientConnection>> _patternSubs = new();

    public void Subscribe(ClientConnection client, string channel)
    {
        var bag = _channelSubs.GetOrAdd(channel, _ => new ConcurrentBag<ClientConnection>());
        bag.Add(client);
        client.ChannelSubscriptions.Add(channel);
    }

    public void Unsubscribe(ClientConnection client, string channel)
    {
        client.ChannelSubscriptions.Remove(channel);
    }

    public void PSubscribe(ClientConnection client, string pattern)
    {
        var bag = _patternSubs.GetOrAdd(pattern, _ => new ConcurrentBag<ClientConnection>());
        bag.Add(client);
        client.PatternSubscriptions.Add(pattern);
    }

    public void PUnsubscribe(ClientConnection client, string pattern)
    {
        client.PatternSubscriptions.Remove(pattern);
    }

    public long Publish(string channel, byte[] message)
    {
        long count = 0;

        if (_channelSubs.TryGetValue(channel, out var subs))
        {
            foreach (var client in subs)
            {
                if (!client.ChannelSubscriptions.Contains(channel)) continue;
                var push = new RespValue.Array(new RespValue[]
                {
                    RespValue.FromBulkString("message"),
                    RespValue.FromBulkString(channel),
                    new RespValue.BulkString(message)
                });
                client.WriteQueue.Writer.TryWrite(push);
                count++;
            }
        }

        foreach (var (pattern, patSubs) in _patternSubs)
        {
            if (!ServerCommands.MatchesGlob(channel, pattern)) continue;
            foreach (var client in patSubs)
            {
                if (!client.PatternSubscriptions.Contains(pattern)) continue;
                var push = new RespValue.Array(new RespValue[]
                {
                    RespValue.FromBulkString("pmessage"),
                    RespValue.FromBulkString(pattern),
                    RespValue.FromBulkString(channel),
                    new RespValue.BulkString(message)
                });
                client.WriteQueue.Writer.TryWrite(push);
                count++;
            }
        }

        return count;
    }

    public void RemoveClient(ClientConnection client)
    {
        client.ChannelSubscriptions.Clear();
        client.PatternSubscriptions.Clear();
    }

    public string[] GetActiveChannels(string pattern)
    {
        return _channelSubs.Keys
            .Where(ch => ServerCommands.MatchesGlob(ch, pattern))
            .Where(ch => _channelSubs.TryGetValue(ch, out var bag) && bag.Any(c => c.ChannelSubscriptions.Contains(ch)))
            .ToArray();
    }

    public long GetSubscriberCount(string channel)
    {
        if (!_channelSubs.TryGetValue(channel, out var bag)) return 0;
        return bag.Count(c => c.ChannelSubscriptions.Contains(channel));
    }

    public long GetPatternCount()
    {
        return _patternSubs.Sum(kvp => kvp.Value.Count(c => c.PatternSubscriptions.Contains(kvp.Key)));
    }
}
