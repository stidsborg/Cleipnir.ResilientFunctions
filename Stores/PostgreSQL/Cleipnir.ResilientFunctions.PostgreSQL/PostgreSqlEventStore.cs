using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
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

    public async Task<FunctionStatus> AppendEvent(FunctionId functionId, StoredEvent storedEvent)
    {
        await using var conn = await CreateConnection();
        await using var batch = new NpgsqlBatch(conn);
        var (eventJson, eventType, idempotencyKey) = storedEvent;
       
        { //append event to event stream sql
            var sql = @$"    
                INSERT INTO {_tablePrefix}rfunctions_events
                    (function_type_id, function_instance_id, position, event_json, event_type, idempotency_key)
                VALUES (
                     $1, $2, 
                     (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}rfunctions_events WHERE function_type_id = $1 AND function_instance_id = $2), 
                     $3, $4, $5
                ) RETURNING position;";
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

        { //get function status
            var sql = @$"    
            SELECT epoch, status
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = $1 AND function_instance_id = $2;";
           
            var command = new NpgsqlBatchCommand(sql)
            {
                Parameters = { 
                    new() {Value = functionId.TypeId.Value},
                    new() {Value = functionId.InstanceId.Value}
                }
            };
            batch.BatchCommands.Add(command);  
        }

        try
        {
            await using var reader = await batch.ExecuteReaderAsync();
            
            _ = await reader.ReadAsync();
            _ = await reader.NextResultAsync();
            
            while (await reader.ReadAsync())
            {
                var epoch = reader.GetInt32(0);
                var status = (Status)reader.GetInt32(1);
                return new FunctionStatus(status, epoch);
            }
        }
        catch (PostgresException e) when (e.SqlState == "23505")
        {
            //read status separately
            return await GetSuspensionStatus(functionId);
        } //ignore entry already exist error
        
        throw new ConcurrentModificationException(functionId); //row must have been deleted concurrently
    }

    public Task<FunctionStatus> AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));
    
    public async Task<FunctionStatus> AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        await using var conn = await CreateConnection();
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable);
        await AppendEvents(functionId, storedEvents, conn, transaction);
        await transaction.CommitAsync();
        
        return await GetSuspensionStatus(functionId);
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

    public Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId)
        => InnerGetEvents(functionId, skip: 0).SelectAsync(events => (IEnumerable<StoredEvent>) events);
    
    private async Task<List<StoredEvent>> InnerGetEvents(FunctionId functionId, int skip)
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

    public EventsSubscription SubscribeToEvents(FunctionId functionId)
    {
        var sync = new object();
        var disposed = false;
        var skip = 0;

        var subscription = new EventsSubscription(
            async () =>
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

        return subscription;
    }
    
    private async Task<FunctionStatus> GetSuspensionStatus(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT epoch, status
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = $1 AND function_instance_id = $2;";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters = { 
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var epoch = reader.GetInt32(0);
            var status = (Status) reader.GetInt32(1);
            return new FunctionStatus(status, epoch);
        }
        
        throw new ConcurrentModificationException(functionId); //row must have been deleted concurrently
    }
}