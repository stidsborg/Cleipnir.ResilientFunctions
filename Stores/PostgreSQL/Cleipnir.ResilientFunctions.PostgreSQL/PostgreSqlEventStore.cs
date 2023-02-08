using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

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
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_events (
                function_type_id VARCHAR(255),
                function_instance_id VARCHAR(255),
                position INT NOT NULL,
                event_json TEXT NOT NULL,
                event_type VARCHAR(255) NOT NULL,   
                idempotency_key VARCHAR(255),          
                PRIMARY KEY (function_type_id, function_instance_id, position)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_{_tablePrefix}rfunctions_events_idempotencykeys
            ON {_tablePrefix}rfunctions_events(function_type_id, function_instance_id, idempotency_key)";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = await CreateConnection();
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_events;";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"TRUNCATE TABLE {_tablePrefix}rfunctions_events;";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task AppendEvent(FunctionId functionId, StoredEvent storedEvent)
    {
        await using var conn = await CreateConnection();
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable);
        var (eventJson, eventType, idempotencyKey) = storedEvent;

        var sql = @$"    
                INSERT INTO {_tablePrefix}rfunctions_events
                    (function_type_id, function_instance_id, position, event_json, event_type, idempotency_key)
                VALUES (
                     $1, $2, 
                     (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}rfunctions_events WHERE function_type_id = $1 AND function_instance_id = $2), 
                     $3, $4, $5
                );";
        await using var command = new NpgsqlCommand(sql, conn, transaction)
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

        try
        {
            await command.ExecuteNonQueryAsync();
            await transaction.CommitAsync();
        }
        catch (PostgresException e) when (e.SqlState == "23505") {}
    }

    public Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));
    
    public async Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        await using var conn = await CreateConnection();
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable);
        await AppendEvents(functionId, storedEvents, conn, transaction);
        await transaction.CommitAsync();
    }

    internal async Task AppendEvents(
        FunctionId functionId, 
        IEnumerable<StoredEvent> storedEvents,
        NpgsqlConnection connection, 
        NpgsqlTransaction? transaction)
    {
        var batch = new NpgsqlBatch(connection, transaction);
        foreach (var (eventJson, eventType, idempotencyKey) in storedEvents)
        {
            string sql;
            if (idempotencyKey == null)
                sql = @$"    
                INSERT INTO {_tablePrefix}rfunctions_events
                    (function_type_id, function_instance_id, position, event_json, event_type, idempotency_key)
                VALUES
                    ($1, $2, (SELECT (COALESCE(MAX(position), -1) + 1) FROM {_tablePrefix}rfunctions_events WHERE function_type_id = $1 AND function_instance_id = $2), $3, $4, $5);";
            else
                sql = @$"
                    INSERT INTO {_tablePrefix}rfunctions_events
                    SELECT $1, $2, (SELECT (COALESCE(MAX(position), -1) + 1) FROM {_tablePrefix}rfunctions_events WHERE function_type_id = $1 AND function_instance_id = $2), $3, $4, $5
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {_tablePrefix}rfunctions_events
                        WHERE function_type_id = $1 AND
                        function_instance_id = $2 AND
                        idempotency_key = $5
                    );";
            
            var command = new NpgsqlBatchCommand(sql)
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
            batch.BatchCommands.Add(command);
        }
        
        await batch.ExecuteNonQueryAsync();
    }

    public async Task Truncate(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        await Truncate(functionId, conn, transaction: null);
    }

    internal async Task<int> Truncate(FunctionId functionId, NpgsqlConnection connection, NpgsqlTransaction? transaction)
    {
        var sql = @$"    
                DELETE FROM {_tablePrefix}rfunctions_events
                WHERE function_type_id = $1 AND function_instance_id = $2;";
        await using var command = new NpgsqlCommand(sql, connection, transaction)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows;
    }

    public async Task<bool> Replace(FunctionId functionId, IEnumerable<StoredEvent> storedEvents, int? expectedEpoch)
    {
        await using var conn = await CreateConnection();
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.RepeatableRead);

        if (expectedEpoch != null && !await IsFunctionAtEpoch(functionId, expectedEpoch.Value, conn, transaction))
            return false;
        
        await Truncate(functionId, conn, transaction);
        await AppendEvents(functionId, storedEvents, conn, transaction);
        await transaction.CommitAsync();
        
        return true;
    }

    private async Task<bool> IsFunctionAtEpoch(FunctionId functionId, int expectedEpoch, NpgsqlConnection conn, NpgsqlTransaction transaction)
    {
        var sql = @$"    
            SELECT COUNT(*)
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = $1 AND function_instance_id = $2 AND epoch = $3;";
        await using var command = new NpgsqlCommand(sql, conn, transaction)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new () {Value = expectedEpoch}
            }
        };

        var count = (long?) await command.ExecuteScalarAsync();
        return count == 1;
    }

    public async Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT event_json, event_type, idempotency_key
            FROM {_tablePrefix}rfunctions_events
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
}