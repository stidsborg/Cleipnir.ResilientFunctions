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
                    EventJson NVARCHAR(MAX) NOT NULL,
                    EventType NVARCHAR(255) NOT NULL,   
                    IdempotencyKey VARCHAR(255),          
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

    public async Task AppendEvent(FunctionId functionId, StoredEvent storedEvent)
    {
        await using var conn = await CreateConnection();

        var sql = @$"    
            INSERT INTO {_tablePrefix}Events
                (FunctionTypeId, FunctionInstanceId, Position, EventJson, EventType, IdempotencyKey)
            VALUES ( 
                @FunctionTypeId, 
                @FunctionInstanceId, 
                (SELECT COUNT(*) FROM {_tablePrefix}events WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId), 
                @EventJson, @EventType, @IdempotencyKey
            );";
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@EventJson", storedEvent.EventJson);
        command.Parameters.AddWithValue("@EventType", storedEvent.EventType);
        command.Parameters.AddWithValue("@IdempotencyKey", storedEvent.IdempotencyKey ?? (object) DBNull.Value);
        await command.ExecuteNonQueryAsync();
    }

    public Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));

    public async Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        await using var conn = await CreateConnection();
        await using var transaction = conn.BeginTransaction();
        foreach (var storedEvent in storedEvents)
        {
            var sql = @$"    
            INSERT INTO {_tablePrefix}Events
                (FunctionTypeId, FunctionInstanceId, Position, EventJson, EventType, IdempotencyKey)
            VALUES ( 
                @FunctionTypeId, 
                @FunctionInstanceId, 
                (SELECT COUNT(*) FROM {_tablePrefix}events WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId), 
                @EventJson, @EventType, @IdempotencyKey
            );";
            await using var command = new SqlCommand(sql, conn, transaction);
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@EventJson", storedEvent.EventJson);
            command.Parameters.AddWithValue("@EventType", storedEvent.EventType);
            command.Parameters.AddWithValue("@IdempotencyKey", storedEvent.IdempotencyKey ?? (object) DBNull.Value);
            await command.ExecuteNonQueryAsync();
        }

        await transaction.CommitAsync();
    }

    public async Task Truncate(FunctionId functionId)
    {
        await using var conn = await CreateConnection();

        var sql = @$"    
            DELETE FROM {_tablePrefix}Events
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT EventJson, EventType, IdempotencyKey
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
            var eventJson = reader.GetString(0);
            var eventType = reader.GetString(1);
            var idempotencyKey = reader.IsDBNull(2) ? null : reader.GetString(2);
            storedEvents.Add(new StoredEvent(eventJson, eventType, idempotencyKey));
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