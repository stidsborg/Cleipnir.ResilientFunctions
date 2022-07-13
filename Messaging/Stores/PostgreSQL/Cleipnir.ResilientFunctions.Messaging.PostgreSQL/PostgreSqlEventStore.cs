using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Npgsql;

namespace Cleipnir.ResilientFunctions.Messaging.PostgreSQL;

public class PostgreSqlEventStore : IEventStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public PostgreSqlEventStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix.ToLower();
    } 

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
    
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}events (
                function_type_id VARCHAR(255),
                function_instance_id VARCHAR(255),
                position INT NOT NULL,
                events_json TEXT NOT NULL,
                PRIMARY KEY (function_type_id, function_instance_id, position)
            );";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = await CreateConnection();
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}events;";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"TRUNCATE TABLE {_tablePrefix}events;";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public Task AppendEvent(FunctionId functionId, StoredEvent storedEvent)
        => AppendEvents(functionId, storedEvents: new[] { storedEvent });

    public Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));
    
    public async Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        await using var conn = await CreateConnection();
        var json = StoredEventsBulkJsonSerializer.SerializeToJson(storedEvents);

        var sql = @$"    
            INSERT INTO {_tablePrefix}events
                (function_type_id, function_instance_id, position, events_json)
            VALUES
                ($1, $2, (SELECT COUNT(*) FROM {_tablePrefix}events WHERE function_type_id = $1 AND function_instance_id = $2), $3);";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = json}
            }
        };
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT events_json
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
            var eventsJson = reader.GetString(0);
            var deserializedStoredEvents = StoredEventsBulkJsonSerializer.DeserializeToStoredEvents(eventsJson);
            storedEvents.AddRange(deserializedStoredEvents);
        }

        return storedEvents;
    }
}