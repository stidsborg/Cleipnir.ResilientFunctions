using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Dapper;
using Npgsql;

namespace Cleipnir.ResilientFunctions.Messaging.PostgreSQL;

public class PostgreSqlEventStore : IEventStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    private bool _initializing;
    private bool _initialized;
    private int _nextSubscriberId;
    private readonly Dictionary<int, Action<FunctionId>> _subscribers = new();
    private readonly object _sync = new();
    private volatile bool _disposed;
    
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

    private readonly TaskCompletionSource _setUpDatabaseSubscriptionTcs = new();
    private bool _setUpDatabaseSubscriptionStarted;
    
    private Task SetUpDatabaseSubscription()
    {
        lock (_sync)
        {
            if (_setUpDatabaseSubscriptionStarted) return _setUpDatabaseSubscriptionTcs.Task;
            _setUpDatabaseSubscriptionStarted = true;
        }
        
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
            
            using (var cmd = new NpgsqlCommand($"LISTEN {_tablePrefix}events", conn)) { 
                cmd.ExecuteNonQuery();
            }
            
            Task.Run(() => _setUpDatabaseSubscriptionTcs.SetResult());

            while (!_disposed) {
                conn.Wait(); // Thread will block here
            }
        })
        {
            Name = $"{_tablePrefix}.ResilientFunctions.Rx.Notification.Listener", 
            IsBackground = true
        };

        thread.Start();
        return _setUpDatabaseSubscriptionTcs.Task;
    }

    public async Task Initialize()
    {
        bool initializing;
        lock (_sync)
        {
            if (_initialized) return;
            initializing = _initializing;
            _initializing = true;
        }

        if (initializing)
        {
            await BusyWait.UntilAsync(() => { lock (_sync) return _initialized; });
            return;
        }

        await using var conn = await CreateConnection();
        await conn.ExecuteAsync(@$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}events (
                function_type_id VARCHAR(255),
                function_instance_id VARCHAR(255),
                position INT NOT NULL,
                event_json TEXT NOT NULL,
                event_type VARCHAR(255) NOT NULL,   
                idempotency_key VARCHAR(255),          
                PRIMARY KEY (function_type_id, function_instance_id, position)
            );" 
        );
        
        await SetUpDatabaseSubscription();
        
        lock (_sync)
            _initialized = true;
    }
    
    public async Task DropUnderlyingTable()
    {
        await using var conn = await CreateConnection();
        await conn.ExecuteAsync(@$"DROP TABLE IF EXISTS {_tablePrefix}events;");
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        await conn.ExecuteAsync(@$"TRUNCATE TABLE {_tablePrefix}events;");
    }
    
    public async Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
    {
        await using var conn = await CreateConnection();

        var sql = @$"    
                INSERT INTO {_tablePrefix}events
                    (function_type_id, function_instance_id, position, event_json, event_type, idempotency_key)
                VALUES
                    ($1, $2, (SELECT COUNT(*) FROM {_tablePrefix}events WHERE function_type_id = $1 AND function_instance_id = $2), $3, $4, $5);";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = eventJson},
                new() {Value = eventType},
                new() {Value = idempotencyKey ?? (object) DBNull.Value}
            }
        };
        await command.ExecuteNonQueryAsync();

        _ = Notify(functionId); //todo improve by using batching instead (for one roundtrip)
    }

    public async Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT event_json, event_type, idempotency_key
            FROM {_tablePrefix}events
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
        
        var storedEvents = new List<StoredEvent>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var eventJson = reader.GetString(0);
            var messageJson = reader.GetString(1);
            var idempotencyKey = reader.IsDBNull(2) ? null : reader.GetString(2);
            storedEvents.Add(new StoredEvent(eventJson, messageJson, idempotencyKey));
        }

        return storedEvents;
    }

    private async Task Notify(FunctionId functionId)
    {
        await using var conn = await CreateConnection();

        var sql = $"SELECT pg_notify('{_tablePrefix}events', $1);";    
            
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

    public void Dispose() => _disposed = true;
}