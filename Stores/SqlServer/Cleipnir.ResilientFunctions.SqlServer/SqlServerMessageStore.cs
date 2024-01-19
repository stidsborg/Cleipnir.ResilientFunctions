using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerMessageStore : IMessageStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public SqlServerMessageStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        var sql = @$"
        CREATE TABLE {_tablePrefix}RFunctions_Messages (
            FunctionTypeId NVARCHAR(255),
            FunctionInstanceId NVARCHAR(255),
            Position INT NOT NULL,
            MessageJson NVARCHAR(MAX) NOT NULL,
            MessageType NVARCHAR(255) NOT NULL,   
            IdempotencyKey NVARCHAR(255),          
            PRIMARY KEY (FunctionTypeId, FunctionInstanceId, Position)
        );
        CREATE UNIQUE INDEX uidx_{_tablePrefix}RFunctions_Messages
            ON {_tablePrefix}RFunctions_Messages (FunctionTypeId, FunctionInstanceId, IdempotencyKey)
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
        var sql = @$"DROP TABLE IF EXISTS {_tablePrefix}RFunctions_Messages;";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"TRUNCATE TABLE {_tablePrefix}RFunctions_Messages;";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<FunctionStatus> AppendMessage(FunctionId functionId, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        await using var transaction = (SqlTransaction) await conn.BeginTransactionAsync(IsolationLevel.Serializable);
        var sql = @$"    
            INSERT INTO {_tablePrefix}RFunctions_Messages
                (FunctionTypeId, FunctionInstanceId, Position, MessageJson, MessageType, IdempotencyKey)
            VALUES ( 
                @FunctionTypeId, 
                @FunctionInstanceId, 
                (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}RFunctions_Messages WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId), 
                @MessageJson, @MessageType, @IdempotencyKey
            );";
        
        await using var command = new SqlCommand(sql, conn, transaction);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@MessageJson", storedMessage.MessageJson);
        command.Parameters.AddWithValue("@MessageType", storedMessage.MessageType);
        command.Parameters.AddWithValue("@IdempotencyKey", storedMessage.IdempotencyKey ?? (object)DBNull.Value);
        try
        {
            await command.ExecuteNonQueryAsync();
            await transaction.CommitAsync();
        }
        catch (SqlException exception) when (exception.Number == 2601) { }
        
        return await GetSuspensionStatus(functionId);
    }

    public Task<FunctionStatus> AppendMessage(FunctionId functionId, string messageJson, string messageType, string? idempotencyKey = null)
        => AppendMessage(functionId, new StoredMessage(messageJson, messageType, idempotencyKey));

    public async Task<FunctionStatus> AppendMessages(FunctionId functionId, IEnumerable<StoredMessage> storedMessages)
    {
        await using var conn = await CreateConnection();
        await using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

        await AppendMessages(functionId, storedMessages, conn, transaction);
        var suspensionStatus = await GetSuspensionStatus(functionId);
        await transaction.CommitAsync();
        return suspensionStatus;
    }
    
    internal async Task AppendMessages(FunctionId functionId, IEnumerable<StoredMessage> storedMessages, SqlConnection connection, SqlTransaction transaction)
    {
        foreach (var storedMessage in storedMessages)
        {
            string sql;
            if (storedMessage.IdempotencyKey == null)
                sql = @$"    
                    INSERT INTO {_tablePrefix}RFunctions_Messages
                        (FunctionTypeId, FunctionInstanceId, Position, MessageJson, MessageType, IdempotencyKey)
                    VALUES ( 
                        @FunctionTypeId, 
                        @FunctionInstanceId, 
                        (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}RFunctions_Messages WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId), 
                        @MessageJson, @MessageType, @IdempotencyKey
                    );";
            else
                sql = @$"
                    INSERT INTO {_tablePrefix}RFunctions_Messages
                    SELECT 
                        @FunctionTypeId, 
                        @FunctionInstanceId, 
                        (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}RFunctions_Messages WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId), 
                        @MessageJson, @MessageType, @IdempotencyKey
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {_tablePrefix}RFunctions_Messages
                        WHERE FunctionTypeId = @FunctionTypeId AND
                        FunctionInstanceId = @FunctionInstanceId AND
                        IdempotencyKey = @IdempotencyKey
                    );";

            await using var command = new SqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@MessageJson", storedMessage.MessageJson);
            command.Parameters.AddWithValue("@MessageType", storedMessage.MessageType);
            command.Parameters.AddWithValue("@IdempotencyKey", storedMessage.IdempotencyKey ?? (object) DBNull.Value);
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
            DELETE FROM {_tablePrefix}RFunctions_Messages
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = 
            transaction == null
                ? new SqlCommand(sql, connection)
                : new SqlCommand(sql, connection, transaction);
        
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        return await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> Replace(FunctionId functionId, IEnumerable<StoredMessage> storedMessages, int? expectedMessageCount)
    {
        await using var conn = await CreateConnection();
        await using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

        if (expectedMessageCount != null)
        {
            var count = await GetMessagesCount(functionId, conn, transaction);
            if (count != expectedMessageCount.Value)
                return false;
        }
        
        await Truncate(functionId, conn, transaction);
        await AppendMessages(functionId, storedMessages, conn, transaction);

        await transaction.CommitAsync();
        return true;
    }

    private async Task<long> GetMessagesCount(FunctionId functionId, SqlConnection conn, SqlTransaction transaction)
    {
        var sql = @$"    
            SELECT COALESCE(MAX(position), -1) + 1 
            FROM {_tablePrefix}RFunctions_Messages
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(sql, conn, transaction);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        
        var count = (int?) await command.ExecuteScalarAsync();
        ArgumentNullException.ThrowIfNull(count);
        
        return count.Value;
    }
    
    public Task<IEnumerable<StoredMessage>> GetMessages(FunctionId functionId)
        => InnerGetMessages(functionId, skip: 0)
            .SelectAsync(messages => (IEnumerable<StoredMessage>) messages);
    
    private async Task<List<StoredMessage>> InnerGetMessages(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT MessageJson, MessageType, IdempotencyKey
            FROM {_tablePrefix}RFunctions_Messages
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Position >= @Position
            ORDER BY Position ASC;";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@Position", skip);
        
        var storedMessages = new List<StoredMessage>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var messageJson = reader.GetString(0);
            var messageType = reader.GetString(1);
            var idempotencyKey = reader.IsDBNull(2) ? null : reader.GetString(2);
            storedMessages.Add(new StoredMessage(messageJson, messageType, idempotencyKey));
        }

        return storedMessages;
    }

    public MessagesSubscription SubscribeToMessages(FunctionId functionId)
    {
        var sync = new object();
        var disposed = false;
        var skip = 0;

        var subscription = new MessagesSubscription(
            pullNewMessages: async () =>
            {
                lock (sync)
                    if (disposed)
                        return ArraySegment<StoredMessage>.Empty;
                
                var messages = await InnerGetMessages(functionId, skip);
                skip += messages.Count;

                return messages;
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

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
    
    private async Task<FunctionStatus> GetSuspensionStatus(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT Epoch, Status
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var epoch = reader.GetInt32(0);
            var status = (Status) reader.GetInt32(1);
            return new FunctionStatus(status, epoch);
        }
        
        throw new ConcurrentModificationException(functionId);
    }
}