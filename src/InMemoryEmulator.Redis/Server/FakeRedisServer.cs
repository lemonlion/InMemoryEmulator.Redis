using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using InMemoryEmulator.Redis.Store;

namespace InMemoryEmulator.Redis.Server;

public sealed class FakeRedisServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly InMemoryRedisStore _store;
    private readonly CommandRouter _router;
    private readonly PubSubBroker _pubSub;
    private readonly CancellationTokenSource _cts = new();
    private readonly ConcurrentBag<ClientConnection> _clients = new();
    private Task? _acceptLoopTask;
    private int _nextClientId;
    private readonly string? _password;

    internal Func<string, RespValue[]?, RespValue?>? FaultInjector { get; set; }

    public int Port { get; private set; }
    public string Host => "127.0.0.1";
    public CommandLog CommandLog { get; }

    internal FakeRedisServer(InMemoryRedisStore store, CommandLog commandLog, string? password)
    {
        _store = store;
        CommandLog = commandLog;
        _password = password;
        _pubSub = new PubSubBroker();
        _router = new CommandRouter(store, _pubSub, commandLog);
        _listener = new TcpListener(IPAddress.Loopback, 0);
    }

    internal CommandRouter Router => _router;

    internal void SetLuaEngine(ILuaScriptEngine engine) => _router.SetLuaEngine(engine);

    public Task StartAsync()
    {
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoopTask = AcceptLoopAsync(_cts.Token);
        return Task.CompletedTask;
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var tcpClient = await _listener.AcceptTcpClientAsync(ct);
                tcpClient.NoDelay = true;
                var clientId = Interlocked.Increment(ref _nextClientId);
                var client = new ClientConnection(clientId, tcpClient);
                if (_password == null) client.Authenticated = true;
                _clients.Add(client);
                _ = HandleClientAsync(client, ct);
            }
            catch (OperationCanceledException) { break; }
            catch (ObjectDisposedException) { break; }
            catch (SocketException) { break; }
        }
    }

    private async Task HandleClientAsync(ClientConnection client, CancellationToken ct)
    {
        _ = ConsumeWriteQueueAsync(client, ct);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var value = await RespParser.ReadValueAsync(client.Reader, ct);
                if (value == null) break;

                var (commandName, args) = ParseCommand(value);
                if (string.IsNullOrEmpty(commandName)) continue;

                // Auth check
                if (_password != null && !client.Authenticated &&
                    commandName != "AUTH" && commandName != "HELLO" && commandName != "QUIT")
                {
                    var err = new RespValue.Error("NOAUTH", "Authentication required.");
                    RespWriter.WriteValue(client.Writer, err);
                    await client.Writer.FlushAsync(ct);
                    continue;
                }

                // Fault injection
                var injected = FaultInjector?.Invoke(commandName, args);
                if (injected != null)
                {
                    RespWriter.WriteValue(client.Writer, injected);
                    await client.Writer.FlushAsync(ct);
                    continue;
                }

                // Transaction queueing
                if (client.InTransaction && commandName != "EXEC" && commandName != "DISCARD" &&
                    commandName != "MULTI" && commandName != "WATCH")
                {
                    client.TransactionQueue!.Add((commandName, args));
                    RespWriter.WriteValue(client.Writer, RespValue.Queued);
                    await client.Writer.FlushAsync(ct);
                    continue;
                }

                // Auth command special handling
                if (commandName == "AUTH")
                {
                    var response = HandleAuth(client, args);
                    RespWriter.WriteValue(client.Writer, response);
                    await client.Writer.FlushAsync(ct);
                    continue;
                }

                var db = _store.GetDatabase(client.SelectedDatabase);
                var context = new CommandContext
                {
                    CommandName = commandName,
                    Arguments = args,
                    Client = client,
                    Database = db,
                    CancellationToken = ct
                };

                var result = await _router.ExecuteAsync(context);
                RespWriter.WriteValue(client.Writer, result);
                await client.Writer.FlushAsync(ct);

                if (commandName == "QUIT") break;
            }
        }
        catch (OperationCanceledException) { }
        catch (IOException) { }
        catch (InvalidOperationException) { }
        finally
        {
            await client.DisposeAsync();
        }
    }

    private async Task ConsumeWriteQueueAsync(ClientConnection client, CancellationToken ct)
    {
        try
        {
            await foreach (var msg in client.WriteQueue.Reader.ReadAllAsync(ct))
            {
                RespWriter.WriteValue(client.Writer, msg);
                await client.Writer.FlushAsync(ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (IOException) { }
        catch (InvalidOperationException) { }
    }

    private RespValue HandleAuth(ClientConnection client, RespValue[] args)
    {
        if (_password == null)
            return new RespValue.Error("ERR", "Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?");

        var providedPassword = args.Length > 0 ? GetString(args[0]) : "";
        // Support AUTH password (Redis 5) and AUTH username password (Redis 6+)
        if (args.Length == 2)
            providedPassword = GetString(args[1]);

        if (providedPassword == _password)
        {
            client.Authenticated = true;
            return RespValue.Ok;
        }

        return new RespValue.Error("WRONGPASS", "invalid username-password pair or user is disabled.");
    }

    private static (string CommandName, RespValue[] Args) ParseCommand(RespValue value)
    {
        if (value is RespValue.Array { Items: { } items } && items.Length > 0)
        {
            var name = GetString(items[0]).ToUpperInvariant();
            var args = items.Skip(1).ToArray();
            return (name, args);
        }
        return ("", System.Array.Empty<RespValue>());
    }

    private static string GetString(RespValue value) => value switch
    {
        RespValue.BulkString { Data: { } data } => Encoding.UTF8.GetString(data),
        RespValue.SimpleString { Value: var v } => v,
        RespValue.Integer { Value: var v } => v.ToString(),
        _ => ""
    };

    public async ValueTask DisposeAsync()
    {
        try { await _cts.CancelAsync(); } catch { /* ignore */ }
        try { _listener.Stop(); } catch { /* ignore */ }
        // Wait for accept loop to finish
        if (_acceptLoopTask != null)
        {
            try { await _acceptLoopTask.WaitAsync(TimeSpan.FromSeconds(2)); }
            catch { /* timed out or cancelled — fine */ }
        }
        // Close all clients
        foreach (var client in _clients)
        {
            try { await client.DisposeAsync(); }
            catch { /* best effort */ }
        }
    }
}
