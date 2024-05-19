using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
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
        CREATE TABLE {_tablePrefix}_Messages (
            FunctionTypeId NVARCHAR(255),
            FunctionInstanceId NVARCHAR(255),
            Position INT NOT NULL,
            MessageJson NVARCHAR(MAX) NOT NULL,
            MessageType NVARCHAR(255) NOT NULL,   
            IdempotencyKey NVARCHAR(255),          
            PRIMARY KEY (FunctionTypeId, FunctionInstanceId, Position)
        );";
        var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }
    
    public async Task DropUnderlyingTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"DROP TABLE IF EXISTS {_tablePrefix}_Messages;";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"TRUNCATE TABLE {_tablePrefix}_Messages;";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<FunctionStatus> AppendMessage(FunctionId functionId, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        
        var sql = @$"    
            INSERT INTO {_tablePrefix}_Messages
                (FunctionTypeId, FunctionInstanceId, Position, MessageJson, MessageType, IdempotencyKey)
            VALUES ( 
                @FunctionTypeId, 
                @FunctionInstanceId, 
                (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}_Messages WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId), 
                @MessageJson, @MessageType, @IdempotencyKey
            );";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@MessageJson", storedMessage.MessageJson);
        command.Parameters.AddWithValue("@MessageType", storedMessage.MessageType);
        command.Parameters.AddWithValue("@IdempotencyKey", storedMessage.IdempotencyKey ?? (object)DBNull.Value);
        try
        {
            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException e)
        {
            if (e.Number == SqlError.UNIQUENESS_INDEX_VIOLATION) //idempotency key already exists
                return await GetSuspensionStatus(functionId, conn);
            if (e.Number != SqlError.DEADLOCK_VICTIM && e.Number != SqlError.UNIQUENESS_VIOLATION) 
                throw;
            
            await conn.DisposeAsync();
            await Task.Delay(Random.Shared.Next(50, 250));
            return await AppendMessage(functionId, storedMessage); 
        }
        
        return await GetSuspensionStatus(functionId, conn);
    }
    
    public async Task<bool> ReplaceMessage(FunctionId functionId, int position, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        
        var sql = @$"    
            UPDATE {_tablePrefix}_Messages
            SET MessageJson = @MessageJson, MessageType = @MessageType, IdempotencyKey = @IdempotencyKey
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Position = @Position";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@Position", position);
        command.Parameters.AddWithValue("@MessageJson", storedMessage.MessageJson);
        command.Parameters.AddWithValue("@MessageType", storedMessage.MessageType);
        command.Parameters.AddWithValue("@IdempotencyKey", storedMessage.IdempotencyKey ?? (object)DBNull.Value);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task Truncate(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        await Truncate(functionId, conn, transaction: null);
    }

    internal async Task<int> Truncate(FunctionId functionId, SqlConnection connection, SqlTransaction? transaction)
    {
        var sql = @$"    
            DELETE FROM {_tablePrefix}_Messages
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = 
            transaction == null
                ? new SqlCommand(sql, connection)
                : new SqlCommand(sql, connection, transaction);
        
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        return await command.ExecuteNonQueryAsync();
    }
    
    public async Task<IReadOnlyList<StoredMessage>> GetMessages(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT MessageJson, MessageType, IdempotencyKey
            FROM {_tablePrefix}_Messages
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Position >= @Position
            ORDER BY Position ASC;";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@Position", skip);
        
        var storedMessages = new List<StoredMessage>();
        try
        {
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var messageJson = reader.GetString(0);
                var messageType = reader.GetString(1);
                var idempotencyKey = reader.IsDBNull(2) ? null : reader.GetString(2);
                storedMessages.Add(new StoredMessage(messageJson, messageType, idempotencyKey));
            }
        }
        catch (SqlException exception)
        {
            if (exception.Number != SqlError.UNIQUENESS_VIOLATION && exception.Number != SqlError.DEADLOCK_VICTIM) throw;

            conn.Dispose();
            return await GetMessages(functionId, skip);
        }

        return storedMessages;
    }

    public async Task<bool> HasMoreMessages(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT COALESCE(MAX(position), -1)
            FROM {_tablePrefix}_Messages
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);

        var maxPosition = (int?) await command.ExecuteScalarAsync();
        if (maxPosition == null)
            return false;

        return maxPosition.Value + 1 > skip;
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
    
    private async Task<FunctionStatus> GetSuspensionStatus(FunctionId functionId, SqlConnection connection)
    {
        var sql = @$"    
            SELECT Epoch, Status
            FROM {_tablePrefix}
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        await using var command = new SqlCommand(sql, connection);
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