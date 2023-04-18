using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

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
        CREATE TABLE {_tablePrefix}RFunctions_Events (
            FunctionTypeId NVARCHAR(255),
            FunctionInstanceId NVARCHAR(255),
            Position INT NOT NULL,
            EventJson NVARCHAR(MAX) NOT NULL,
            EventType NVARCHAR(255) NOT NULL,   
            IdempotencyKey NVARCHAR(255),          
            PRIMARY KEY (FunctionTypeId, FunctionInstanceId, Position)
        );
        CREATE UNIQUE INDEX uidx_{_tablePrefix}RFunctions_Events
            ON {_tablePrefix}RFunctions_Events (FunctionTypeId, FunctionInstanceId, IdempotencyKey)
            WHERE IdempotencyKey IS NOT NULL;";
        var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }
    
    public async Task DropUnderlyingTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"DROP TABLE IF EXISTS {_tablePrefix}RFunctions_Events;";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"TRUNCATE TABLE {_tablePrefix}RFunctions_Events;";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<SuspensionStatus> AppendEvent(FunctionId functionId, StoredEvent storedEvent)
    {
        await using var conn = await CreateConnection();
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable);
        var sql = @$"    
            INSERT INTO {_tablePrefix}RFunctions_Events
                (FunctionTypeId, FunctionInstanceId, Position, EventJson, EventType, IdempotencyKey)
            VALUES ( 
                @FunctionTypeId, 
                @FunctionInstanceId, 
                (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}RFunctions_Events WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId), 
                @EventJson, @EventType, @IdempotencyKey
            );";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@EventJson", storedEvent.EventJson);
        command.Parameters.AddWithValue("@EventType", storedEvent.EventType);
        command.Parameters.AddWithValue("@IdempotencyKey", storedEvent.IdempotencyKey ?? (object)DBNull.Value);
        try
        {
            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException exception) when (exception.Number == 2601)
        {
            return new SuspensionStatus(Suspended: false, Epoch: null);
        }
        
        var suspensionStatus = await GetSuspensionStatus(functionId, conn);
        await transaction.CommitAsync();
        
        return suspensionStatus;
    }

    public Task<SuspensionStatus> AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));

    public async Task<SuspensionStatus> AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        await using var conn = await CreateConnection();
        await using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

        await AppendEvents(functionId, storedEvents, conn, transaction);
        var suspensionStatus = await GetSuspensionStatus(functionId, conn);
        await transaction.CommitAsync();
        return suspensionStatus;
    }
    
    internal async Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents, SqlConnection connection, SqlTransaction transaction)
    {
        foreach (var storedEvent in storedEvents)
        {
            string sql;
            if (storedEvent.IdempotencyKey == null)
                sql = @$"    
                    INSERT INTO {_tablePrefix}RFunctions_Events
                        (FunctionTypeId, FunctionInstanceId, Position, EventJson, EventType, IdempotencyKey)
                    VALUES ( 
                        @FunctionTypeId, 
                        @FunctionInstanceId, 
                        (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}RFunctions_Events WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId), 
                        @EventJson, @EventType, @IdempotencyKey
                    );";
            else
                sql = @$"
                    INSERT INTO {_tablePrefix}RFunctions_Events
                    SELECT 
                        @FunctionTypeId, 
                        @FunctionInstanceId, 
                        (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}RFunctions_Events WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId), 
                        @EventJson, @EventType, @IdempotencyKey
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {_tablePrefix}RFunctions_Events
                        WHERE FunctionTypeId = @FunctionTypeId AND
                        FunctionInstanceId = @FunctionInstanceId AND
                        IdempotencyKey = @IdempotencyKey
                    );";

            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@EventJson", storedEvent.EventJson);
            command.Parameters.AddWithValue("@EventType", storedEvent.EventType);
            command.Parameters.AddWithValue("@IdempotencyKey", storedEvent.IdempotencyKey ?? (object) DBNull.Value);
            await command.ExecuteNonQueryAsync();
        }
    }

    public async Task Truncate(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        await Truncate(functionId, conn, transaction: null);
    }

    internal async Task<int> Truncate(FunctionId functionId, SqlConnection connection, SqlTransaction? transaction)
    {
        var sql = @$"    
            DELETE FROM {_tablePrefix}RFunctions_Events
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = 
            transaction == null
                ? new SqlCommand(sql, connection)
                : new SqlCommand(sql, connection, transaction);
        
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        return await command.ExecuteNonQueryAsync();
    }

    public Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId)
        => InnerGetEvents(functionId, skip: 0)
            .SelectAsync(events => (IEnumerable<StoredEvent>) events);
        

    private async Task<List<StoredEvent>> InnerGetEvents(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT EventJson, EventType, IdempotencyKey
            FROM {_tablePrefix}RFunctions_Events
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

    public Task<EventsSubscription> SubscribeToEvents(FunctionId functionId)
    {
        var sync = new object();
        var disposed = false;
        var skip = 0;

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

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
    
    private async Task<SuspensionStatus> GetSuspensionStatus(FunctionId functionId, SqlConnection connection)
    {
        var sql = @$"    
            SELECT Epoch, Status
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var epoch = reader.GetInt32(0);
            var status = (Status) reader.GetInt32(1);
            return new SuspensionStatus(Suspended: status == Status.Suspended, Epoch: epoch);
        }
        
        throw new ConcurrentModificationException(functionId);
    }
}