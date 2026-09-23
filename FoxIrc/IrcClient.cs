using System.Net.Sockets;

namespace FoxIrc;

public class IrcClient : IAsyncDisposable
{
    private TcpClient? _client;
    private NetworkStream? _stream;
    private StreamReader? _reader;
    private StreamWriter? _writer;
    private CancellationTokenSource? _cts;
    private Task? _listener;

    public string Server { get; }
    public int Port { get; }
    public string? Pass { get; private set; }

    public string Nick { get; private set; }
    public string User { get; private set; }
    public string Real { get; private set; }

    public bool IsConnected => _client?.Connected ?? false;

    public event Action<IrcClient, string>? RawDataReceived;
    public event Action<IrcClient, string, string, string>? MessageReceived;
    public event Action<IrcClient, string>? SystemLog;

    public IrcClient(string server, string nick, string user, string real, string? pass = null, int port = 6667)
    {
        Server = server;
        Port = port;
        Pass = pass;

        Nick = nick;
        User = user;
        Real = real;
    }

    public async Task ConnectAsync()
    {
        if (IsConnected) return;

        _cts = new();
        _client = new();

        Log(LogLevel.Info, $"Connecting to {Server}:{Port} as {Nick}");
        await _client.ConnectAsync(Server, Port);

        _stream = _client.GetStream();
        _reader = new(_stream);
        _writer = new(_stream) { AutoFlush = true };

        if (Pass != null)
            await SendRawAsync($"PASS {Pass}");

        await SendRawAsync($"NICK {Nick}");
        await SendRawAsync($"USER {User} 0 * :{Real}");

        _listener = Task.Run(() => ListenAsync(_cts.Token));
    }

    public async Task DisconnectAsync(string reason = "Client Terminated Session")
    {
        if (!IsConnected) return;

        Log(LogLevel.Info, "Disconnecting...");
        try
        {
            await SendRawAsync($"QUIT :{reason}");
        }
        catch { }

        _cts?.Cancel();
        _client?.Close();
        _client?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync();
        GC.SuppressFinalize(this);
    }

    #region Channel Commands
    public async Task JoinChannelAsync(string channel)
    {
        string formattedChannel = channel.StartsWith('#') ? channel : $"#{channel}";
        await SendRawAsync($"JOIN {formattedChannel}");
        Log(LogLevel.Info, $"Joining channel {formattedChannel}");
    }

    public async Task LeaveChannelAsync(string channel)
    {
        string formattedChannel = channel.StartsWith('#') ? channel : $"#{channel}";
        await SendRawAsync($"PART {formattedChannel}");
        Log(LogLevel.Info, $"Left channel {formattedChannel}");
    }

    public async Task GetTopicAsync(string channel) =>
        await SendRawAsync($"TOPIC {channel}");

    public async Task SetTopicAsync(string channel, string topic = "") =>
        await SendRawAsync($"TOPIC {channel} :{topic}");

    public async Task GetChannelsAsync() =>
        await SendRawAsync("LIST");

    public async Task GetChannelsAsync(string args) =>
        await SendRawAsync($"LIST {args}");
    #endregion

    public async Task SendMessageAsync(string target, string message) =>
        await SendRawAsync($"PRIVMSG {target} :{message}");

    public async Task SendNoticeAsync(string target, string notice) =>
        await SendRawAsync($"NOTICE {target} :{notice}");

    public async Task AwayAsync(string? text = null) =>
        await SendRawAsync($"AWAY{(text == null ? "" : $" {text}")}");

    #region Info Commands
    public async Task GetNamesAsync(string channels) =>
        await SendRawAsync($"NAMES {channels}");

    public async Task GetMotdAsync(string? target = null) =>
        await SendRawAsync($"MOTD{(target == null ? "" : $" {target}")}");

    public async Task GetServerTimeAsync(string? server = null) =>
        await SendRawAsync($"TIME{(server == null ? "" : $" {server}")}");

    public async Task WhoisAsync(string nick, string? target = null) =>
        await SendRawAsync($"WHOIS{(target == null ? "" : $" {target}")} {nick}");

    public async Task WhowasAsync(string nick, int count = -1) =>
        await SendRawAsync($"WHOWAS {nick} {count}");
    #endregion

    #region Operator Commands
    public async Task GetAdmin(string? target = null) =>
        await SendRawAsync($"ADMIN{(target == null ? "" : $" {target}")}");

    public async Task LoginOpAsync(string user, string pass) =>
        await SendRawAsync($"OPER {user} {pass}");

    public async Task KickAsync(string channel, string user, string? comment = null) =>
        await SendRawAsync($"KICK {channel} {user}{(comment == null ? "" : $" :{comment}")}");

    public async Task KillAsync(string nick, string comment) =>
        await SendRawAsync($"KILL {nick} {comment}");
    #endregion

    public async Task SendRawAsync(string rawLine)
    {
        if (!IsConnected) return;
        if (_writer != null)
            await _writer.WriteLineAsync(rawLine);
    }

    private async Task ListenAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested && _reader != null)
            {
                string? line = await _reader.ReadLineAsync(ct);
                if (line == null) break;

                RawDataReceived?.Invoke(this, line);

                if (line.StartsWith("PING"))
                {
                    string pingPayload = line[5..];
                    await SendRawAsync($"PONG {pingPayload}");
                }
                else if (line.Contains(" PRIVMSG "))
                    ParsePrivMsg(line);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Log(LogLevel.Error, $"ListenAsync: {ex.Message}");
        }
    }

    private void ParsePrivMsg(string rawLine)
    {
        try
        {
            int nickEnd = rawLine.IndexOf('!');
            if (nickEnd <= 1) return;

            string sender = rawLine[1..nickEnd];
            int msgIdx = rawLine.IndexOf(" PRIVMSG ");
            string remainder = rawLine[(msgIdx + 9)..];
            int targetEnd = remainder.IndexOf(" :");

            if (targetEnd > 0)
            {
                string target = remainder[..targetEnd];
                string content = remainder[(targetEnd + 2)..];
                MessageReceived?.Invoke(this, sender, target, content);
            }
        }
        catch { }
    }

    private void Log(LogLevel level, string message) => SystemLog?.Invoke(this, $"[{level}] {message}");
}
