using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Dapper;
using Npgsql;

namespace Cleipnir.ResilientFunctions.Messaging.PostgreSQL;

public class PostgreSqlEventStore : IEventStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix; //todo use this prefix

    private bool _initialized;
    private int _nextSubscriberId;
    private readonly Dictionary<int, Action<FunctionId>> _subscribers = new();
    private readonly object _sync = new();
    
    public PostgreSqlEventStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    } 

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
    
    public Task<IDisposable> SubscribeToChanges(Action<FunctionId> subscriber)
    {
        lock (_subscribers)
        {
            var observerId = _nextSubscriberId++;
            _subscribers[observerId] = subscriber;

            return new Subscription(this, observerId).CastTo<IDisposable>().ToTask();
        }
    }
    
    private Task SetUpDatabaseSubscription()
    {
        var tcs = new TaskCompletionSource();
        
        var thread = new Thread(_ =>
        {
            var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            conn.Notification += (sender, args) =>
            {
                var functionIdStringArr = args.Payload.Split("@");
                var functionId = new FunctionId(functionIdStringArr[1], functionIdStringArr[0]);
                List<Action<FunctionId>> subscribers;
                lock (_sync)
                    subscribers = _subscribers.Values.ToList();

                foreach (var observer in subscribers)
                    observer(functionId);
            };
            
            using (var cmd = new NpgsqlCommand("LISTEN cleipnir_rx_notifications", conn)) { //todo change channel_name to table name
                cmd.ExecuteNonQuery();
            }
            
            Task.Run(() => tcs.SetResult());

            while (true) {
                conn.Wait(); // Thread will block here
            }
        })
        {
            Name = "ResilientFunctions.Rx.Notification.Listener", //todo add prefix?
            IsBackground = true
        };

        thread.Start();
        return tcs.Task;
    }
    
    public EventSource CreateMessagesInstance(FunctionId functionId) => new EventSource(functionId, this);

    public async Task Initialize()
    {
        lock (_sync)
        {
            if (_initialized) throw new InvalidOperationException("Instance already initialized");
            _initialized = true;
        }
        
        await using var conn = await CreateConnection();
        await conn.ExecuteAsync(@"
            CREATE TABLE IF NOT EXISTS messages (
                function_type_id VARCHAR(255),
                function_instance_id VARCHAR(255),
                position INT,
                message_json TEXT,
                message_type VARCHAR(255),             
                PRIMARY KEY (function_type_id, function_instance_id, position)
            );" 
        );
        
        await SetUpDatabaseSubscription();
    }
    
    public async Task DropUnderlyingTable()
    {
        await using var conn = await CreateConnection();
        await conn.ExecuteAsync(@"DROP TABLE IF EXISTS messages;");
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        await conn.ExecuteAsync(@"TRUNCATE TABLE messages;");
    }
    
    public async Task AppendEvent(FunctionId functionId, object @event)
    {
        var messageJson = JsonSerializer.Serialize(@event, @event.GetType());
        var messageType = @event.GetType().SimpleQualifiedName();

        await using var conn = await CreateConnection();

        var sql = @"    
                INSERT INTO messages
                    (function_type_id, function_instance_id, position, message_json, message_type)
                VALUES
                    ($1, $2, (SELECT COUNT(*) FROM messages WHERE function_type_id = $1 AND function_instance_id = $2), $3, $4);";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = messageJson},
                new() {Value = messageType},
                new() {Value = functionId.ToString()} //todo consider best way to serialize and deserialize function id
            }
        };
        await command.ExecuteNonQueryAsync();

        _ = Notify(functionId); //todo improve by using batching instead (for one roundtrip)
    }

    public async Task<IEnumerable<object>> GetEvents(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @"    
            SELECT message_json, message_type
            FROM messages
            WHERE function_type_id = $1 AND function_instance_id = $2 AND position >= $3
            ORDER BY position ASC;";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new () {Value = skip}
            }
        };
        
        var messages = new List<object>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var messageJson = reader.GetString(0);
            var messageType = reader.GetString(1);
            var message = JsonSerializer.Deserialize(messageJson, Type.GetType(messageType, throwOnError: true)!)!;
            messages.Add(message);
        }

        return messages;
    }

    private async Task Notify(FunctionId functionId)
    {
        await using var conn = await CreateConnection();

        var sql = "SELECT pg_notify('cleipnir_rx_notifications', $1);";    
            
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.ToString()} //todo consider best way to serialize and deserialize function id
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private class Subscription : IDisposable
    {
        private readonly PostgreSqlEventStore _store;
        private readonly int _subscriberId;

        public Subscription(PostgreSqlEventStore store, int subscriberId)
        {
            _store = store;
            _subscriberId = subscriberId;
        }

        public void Dispose()
        {
            lock (_store._sync)
                _store._subscribers.Remove(_subscriberId);
        }
    }
}