using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;

namespace InMemoryEmulator.Redis.Server;

internal sealed class ClientConnection : IAsyncDisposable
{
    private readonly TcpClient _tcpClient;
    private readonly NetworkStream _stream;

    public int Id { get; }
    public int SelectedDatabase { get; set; }
    public string? ClientName { get; set; }
    public string? LibraryName { get; set; }
    public string? LibraryVersion { get; set; }
    public bool Authenticated { get; set; }

    public bool InTransaction { get; set; }
    public List<(string CommandName, RespValue[] Args)>? TransactionQueue { get; set; }
    public Dictionary<string, long>? WatchedKeyVersions { get; set; }

    public HashSet<string> ChannelSubscriptions { get; } = new(StringComparer.Ordinal);
    public HashSet<string> PatternSubscriptions { get; } = new(StringComparer.Ordinal);
    public bool IsInSubscribeMode => ChannelSubscriptions.Count > 0 || PatternSubscriptions.Count > 0;

    public PipeReader Reader { get; }
    public PipeWriter Writer { get; }
    public Channel<RespValue> WriteQueue { get; } = Channel.CreateUnbounded<RespValue>();

    public ClientConnection(int id, TcpClient tcpClient)
    {
        Id = id;
        _tcpClient = tcpClient;
        _stream = tcpClient.GetStream();
        Reader = PipeReader.Create(_stream);
        Writer = PipeWriter.Create(_stream);
    }

    public async ValueTask DisposeAsync()
    {
        WriteQueue.Writer.TryComplete();
        await Reader.CompleteAsync();
        await Writer.CompleteAsync();
        _stream.Dispose();
        _tcpClient.Dispose();
    }
}
