using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using MySql.Data.MySqlClient;

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
            CREATE TABLE IF NOT EXISTS {_tablePrefix}events (
                function_type_id VARCHAR(255),
                function_instance_id VARCHAR(255),
                position INT NOT NULL,
                event_json TEXT NOT NULL,
                event_type VARCHAR(255) NOT NULL,   
                idempotency_key VARCHAR(255),          
                PRIMARY KEY (function_type_id, function_instance_id, position)
            );";
        var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropUnderlyingTableIfExists()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}events";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"TRUNCATE TABLE {_tablePrefix}events;";
        var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task AppendEvent(FunctionId functionId, StoredEvent storedEvent)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var (eventJson, eventType, idempotencyKey) = storedEvent;
        
        var sql = @$"    
                INSERT INTO {_tablePrefix}events
                    (function_type_id, function_instance_id, position, event_json, event_type, idempotency_key)
                SELECT ?, ?, COUNT(*), ?, ?, ? 
                FROM {_tablePrefix}events
                WHERE function_type_id = ? AND function_instance_id = ?";
        await using var command = new MySqlCommand(sql, conn)
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
        await command.ExecuteNonQueryAsync();
    }

    public Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));
    
    public async Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        var count = await GetNumberOfEvents(functionId);
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        
        var transaction = await conn.BeginTransactionAsync();
        {
            foreach (var (eventJson, eventType, idempotencyKey) in storedEvents)
            {
                var sql = @$"    
                INSERT INTO {_tablePrefix}events
                    (function_type_id, function_instance_id, position, event_json, event_type, idempotency_key)
                VALUES
                    (?, ?, ?, ?, ?, ?);";
           
                await using var command = new MySqlCommand(sql, conn, transaction)
                {
                    Parameters =
                    {
                        new() {Value = functionId.TypeId.Value},
                        new() {Value = functionId.InstanceId.Value},
                        new() {Value = count++},
                        new() {Value = eventJson},
                        new() {Value = eventType},
                        new() {Value = idempotencyKey ?? (object) DBNull.Value}
                    }
                };

                await command.ExecuteNonQueryAsync();
            }
        }

        await transaction.CommitAsync();
    }

    private async Task<long> GetNumberOfEvents(FunctionId functionId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"SELECT COUNT(*) FROM {_tablePrefix}events WHERE function_type_id = ? AND function_instance_id = ?";
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
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;

        var sql = @$"    
                DELETE FROM {_tablePrefix}events
                WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"    
            SELECT event_json, event_type, idempotency_key
            FROM {_tablePrefix}events
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
}