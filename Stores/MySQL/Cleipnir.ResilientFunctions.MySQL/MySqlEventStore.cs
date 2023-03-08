using System.Data;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlEventStore : IEventStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;
    
    public MySqlEventStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix.ToLower();
    }

    public async Task Initialize()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_events (
                function_type_id VARCHAR(255),
                function_instance_id VARCHAR(255),
                position INT NOT NULL,
                event_json TEXT NOT NULL,
                event_type VARCHAR(255) NOT NULL,   
                idempotency_key VARCHAR(255),          
                PRIMARY KEY (function_type_id, function_instance_id, position),
                UNIQUE INDEX (function_type_id, function_instance_id, idempotency_key)
            );";
        var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_events";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"TRUNCATE TABLE {_tablePrefix}rfunctions_events;";
        var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task AppendEvent(FunctionId functionId, StoredEvent storedEvent)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.RepeatableRead);
        var (eventJson, eventType, idempotencyKey) = storedEvent;
        
        var sql = @$"    
                INSERT INTO {_tablePrefix}rfunctions_events
                    (function_type_id, function_instance_id, position, event_json, event_type, idempotency_key)
                SELECT ?, ?, COALESCE(MAX(position), -1) + 1, ?, ?, ? 
                FROM {_tablePrefix}rfunctions_events
                WHERE function_type_id = ? AND function_instance_id = ?";
        await using var command = new MySqlCommand(sql, conn, transaction)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = eventJson},
                new() {Value = eventType},
                new() {Value = idempotencyKey ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        try
        {
            await command.ExecuteNonQueryAsync();
            await transaction.CommitAsync();
        }
        catch (MySqlException e) when (e.Number == 1062)
        {
        } //ignore duplicate idempotency key
        catch (MySqlException e) when (e.Number == 1213)
        {
            await transaction.RollbackAsync();
            await AppendEvent(functionId, storedEvent);
        }
    }

    public Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));
    
    public async Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        var existingCount = await GetNumberOfEvents(functionId);
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var transaction = await conn.BeginTransactionAsync(IsolationLevel.RepeatableRead);

        try
        {
            await AppendEvents(functionId, storedEvents, existingCount, conn, transaction);
            await transaction.CommitAsync();    
        }
        catch (MySqlException e) when (e.Number == 1213)
        {
            await transaction.RollbackAsync();
            await AppendEvents(functionId, storedEvents);
        }
    }

    internal async Task AppendEvents(
        FunctionId functionId,
        IEnumerable<StoredEvent> storedEvents,
        long existingCount,
        MySqlConnection connection,
        MySqlTransaction transaction)
    {
        var existingIdempotencyKeys = new HashSet<string>();
        storedEvents = storedEvents.ToList();
        if (storedEvents.Any(se => se.IdempotencyKey != null))
            existingIdempotencyKeys = await GetExistingIdempotencyKeys(functionId, connection, transaction);
        
        foreach (var (eventJson, eventType, idempotencyKey) in storedEvents)
        {
            if (idempotencyKey != null && existingIdempotencyKeys.Contains(idempotencyKey))
                continue;
            if (idempotencyKey != null)
                existingIdempotencyKeys.Add(idempotencyKey);
            
            var sql = @$"    
                INSERT INTO {_tablePrefix}rfunctions_events
                    (function_type_id, function_instance_id, position, event_json, event_type, idempotency_key)
                VALUES
                    (?, ?, ?, ?, ?, ?);";
           
            await using var command = new MySqlCommand(sql, connection, transaction)
            {
                Parameters =
                {
                    new() {Value = functionId.TypeId.Value},
                    new() {Value = functionId.InstanceId.Value},
                    new() {Value = existingCount++},
                    new() {Value = eventJson},
                    new() {Value = eventType},
                    new() {Value = idempotencyKey ?? (object) DBNull.Value}
                }
            };

            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task<HashSet<string>> GetExistingIdempotencyKeys(
        FunctionId functionId,
        MySqlConnection connection, MySqlTransaction transaction)
    {
        var sql = @$"    
            SELECT idempotency_key
            FROM {_tablePrefix}rfunctions_events
            WHERE function_type_id = ? AND function_instance_id = ? AND idempotency_key IS NOT NULL";
        await using var command = new MySqlCommand(sql, connection, transaction)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
            }
        };
        
        var idempotencyKeys = new HashSet<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var idempotencyKey = reader.GetString(0);
            idempotencyKeys.Add(idempotencyKey);
        }

        return idempotencyKeys;
    }

    public async Task<long> GetNumberOfEvents(FunctionId functionId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"SELECT COUNT(*) FROM {_tablePrefix}rfunctions_events WHERE function_type_id = ? AND function_instance_id = ?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
            }
        };
        return (long) (await command.ExecuteScalarAsync())!;
    }

    public async Task Truncate(FunctionId functionId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await Truncate(functionId, conn, transaction: null);
    }

    internal async Task<int> Truncate(FunctionId functionId, MySqlConnection connection, MySqlTransaction? transaction)
    {
        var sql = @$"    
                DELETE FROM {_tablePrefix}rfunctions_events
                WHERE function_type_id = ? AND function_instance_id = ?";
        
        await using var command =
            transaction == null
                ? new MySqlCommand(sql, connection)
                : new MySqlCommand(sql, connection, transaction);

        command.Parameters.Add(new() { Value = functionId.TypeId.Value });
        command.Parameters.Add(new() { Value = functionId.InstanceId.Value });
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows;
    }
    
    internal async Task<bool> Replace(
        FunctionId functionId, IEnumerable<StoredEvent> storedEvents, int? expectedCount, 
        MySqlConnection conn, MySqlTransaction transaction)
    {
        var affectedRows = await Truncate(functionId, conn, transaction);
        if (expectedCount != null && affectedRows != expectedCount)
            return false;
        
        await AppendEvents(functionId, storedEvents, existingCount: 0, conn, transaction);
        return true;
    }

    public Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId)
        => InnerGetEvents(functionId, skip: 0).SelectAsync(events => (IEnumerable<StoredEvent>)events);
    
    private async Task<List<StoredEvent>> InnerGetEvents(FunctionId functionId, int skip)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"    
            SELECT event_json, event_type, idempotency_key
            FROM {_tablePrefix}rfunctions_events
            WHERE function_type_id = ? AND function_instance_id = ? AND position >= ?
            ORDER BY position ASC;";
        await using var command = new MySqlCommand(sql, conn)
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
    
    public Task<EventsSubscription> SubscribeToEvents(FunctionId functionId)
    {
        var sync = new object();
        var skip = 0;
        var disposed = false;


        var subscription = new EventsSubscription(
            pullEvents: async () =>
            {
                lock (sync)
                    if (disposed)
                        return ArraySegment<StoredEvent>.Empty;
                
                var events = await InnerGetEvents(functionId, skip);
                skip += events.Count;
                return events;
            },
            dispose: () =>
            {
                lock (sync)
                    disposed = true;

                return ValueTask.CompletedTask;
            }
        );

        return Task.FromResult(subscription);
    }
}