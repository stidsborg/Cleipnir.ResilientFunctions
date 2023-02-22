﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerEventStore : IEventStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;
    private readonly UnhandledExceptionHandler? _unhandledExceptionHandler;

    public SqlServerEventStore(string connectionString, string tablePrefix = "", UnhandledExceptionHandler? unhandledExceptionHandler = null)
    {
        _connectionString = connectionString;
        _unhandledExceptionHandler = unhandledExceptionHandler;
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

    public async Task AppendEvent(FunctionId functionId, StoredEvent storedEvent)
    {
        await using var conn = await CreateConnection();

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
        command.Parameters.AddWithValue("@IdempotencyKey", storedEvent.IdempotencyKey ?? (object) DBNull.Value);
        try
        {
            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException exception) when (exception.Number == 2601) {}
    }

    public Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null)
        => AppendEvent(functionId, new StoredEvent(eventJson, eventType, idempotencyKey));

    public async Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents)
    {
        await using var conn = await CreateConnection();
        await using var transaction = conn.BeginTransaction();

        await AppendEvents(functionId, storedEvents, conn, transaction);
        
        await transaction.CommitAsync();
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

    public async Task<bool> Replace(FunctionId functionId, IEnumerable<StoredEvent> storedEvents, int? expectedCount)
    {
        await using var conn = await CreateConnection();
        await using var transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);

        var affectedRows = await Truncate(functionId, conn, transaction);
        if (expectedCount != null && affectedRows != expectedCount)
            return false;
        
        await AppendEvents(functionId, storedEvents, conn, transaction);

        await transaction.CommitAsync();
        return true;
    }

    public Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip)
        => InnerGetEvents(functionId, skip)
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

    public Task<IAsyncDisposable> SubscribeToEvents(FunctionId functionId, Action<IEnumerable<StoredEvent>> callback, TimeSpan? pullFrequency)
    {
        var sync = new object();
        var disposed = false;
        pullFrequency ??= TimeSpan.FromMilliseconds(250);

        var subscription = new EventsSubscription(
            functionId,
            callback, 
            dispose: () =>
            {
                lock (sync)
                    disposed = true;

                return ValueTask.CompletedTask;
            },
            _unhandledExceptionHandler
        );
        
        Task.Run(async () =>
        {
            var skip = 0;

            while (true)
            {
                var events = await InnerGetEvents(functionId, skip);
                skip += events.Count;
                
                if (events.Count > 0)
                    subscription.DeliverNewEvents(events);

                await Task.Delay(pullFrequency.Value);

                lock (sync)
                    if (disposed)
                        return;
            }
        });

        return Task.FromResult((IAsyncDisposable) subscription);
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}