using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.Messaging.SqlServer;

public class SqlServerEventStore : IEventStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public SqlServerEventStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        var sql = @$"
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{_tablePrefix}Events' and xtype='U')
                CREATE TABLE {_tablePrefix}Events (
                    FunctionTypeId NVARCHAR(255),
                    FunctionInstanceId NVARCHAR(255),
                    Position INT NOT NULL,
                    EventsJson NVARCHAR(MAX) NOT NULL,
                    PRIMARY KEY (FunctionTypeId, FunctionInstanceId, Position)
                );";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task DropUnderlyingTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            IF EXISTS (SELECT * FROM sysobjects WHERE name='{_tablePrefix}Events' and xtype='U')
                DROP TABLE IF EXISTS {_tablePrefix}Events;";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"TRUNCATE TABLE {_tablePrefix}Events;";
        var command = new SqlCommand(sql, conn);
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
            INSERT INTO {_tablePrefix}Events
                (FunctionTypeId, FunctionInstanceId, Position, EventsJson)
            VALUES ( 
                @FunctionTypeId, 
                @FunctionInstanceId, 
                (SELECT COUNT(*) FROM {_tablePrefix}events WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId), 
                @EventsJson
            );";
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@EventsJson", json);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT EventsJson
            FROM {_tablePrefix}Events
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Position >= @Position
            ORDER BY Position ASC;";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@Position", skip);
        
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

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}