using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
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

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        _initializeSql ??= @$"
        CREATE TABLE {_tablePrefix}_Messages (
            FlowType INT,
            FlowInstance UNIQUEIDENTIFIER,
            Position INT NOT NULL,
            MessageJson VARBINARY(MAX) NOT NULL,
            MessageType VARBINARY(MAX) NOT NULL,   
            IdempotencyKey NVARCHAR(255),          
            PRIMARY KEY (FlowType, FlowInstance, Position)
        );";
        var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_Messages;";
        var command = new SqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _appendMessageSql;
    public async Task<FunctionStatus?> AppendMessage(StoredId storedId, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        
        _appendMessageSql ??= @$"    
            INSERT INTO {_tablePrefix}_Messages
                (FlowType, FlowInstance, Position, MessageJson, MessageType, IdempotencyKey)
            VALUES ( 
                @FlowType, 
                @FlowInstance, 
                (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}_Messages WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance), 
                @MessageJson, @MessageType, @IdempotencyKey
            );";
        
        await using var command = new SqlCommand(_appendMessageSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
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
                return await GetSuspensionStatus(storedId, conn);
            if (e.Number != SqlError.DEADLOCK_VICTIM && e.Number != SqlError.UNIQUENESS_VIOLATION) 
                throw;
            
            await conn.DisposeAsync();
            await Task.Delay(Random.Shared.Next(50, 250));
            return await AppendMessage(storedId, storedMessage); 
        }
        
        return await GetSuspensionStatus(storedId, conn);
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, int position, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        
        _replaceMessageSql ??= @$"    
            UPDATE {_tablePrefix}_Messages
            SET MessageJson = @MessageJson, MessageType = @MessageType, IdempotencyKey = @IdempotencyKey
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Position = @Position";
        
        await using var command = new SqlCommand(_replaceMessageSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@Position", position);
        command.Parameters.AddWithValue("@MessageJson", storedMessage.MessageJson);
        command.Parameters.AddWithValue("@MessageType", storedMessage.MessageType);
        command.Parameters.AddWithValue("@IdempotencyKey", storedMessage.IdempotencyKey ?? (object)DBNull.Value);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _truncateSql;
    public async Task Truncate(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= @$"    
            DELETE FROM {_tablePrefix}_Messages
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";

        await using var command = new SqlCommand(_truncateSql, conn);
        
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        await command.ExecuteNonQueryAsync();
    }

    private string? _getMessagesSql;
    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, int skip)
    {
        await using var conn = await CreateConnection();
        _getMessagesSql ??= @$"    
            SELECT MessageJson, MessageType, IdempotencyKey
            FROM {_tablePrefix}_Messages
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Position >= @Position
            ORDER BY Position ASC;";
        
        await using var command = new SqlCommand(_getMessagesSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@Position", skip);
        
        var storedMessages = new List<StoredMessage>();
        try
        {
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var messageJson = (byte[]) reader.GetValue(0);
                var messageType = (byte[]) reader.GetValue(1);
                var idempotencyKey = reader.IsDBNull(2) ? null : reader.GetString(2);
                storedMessages.Add(new StoredMessage(messageJson, messageType, idempotencyKey));
            }
        }
        catch (SqlException exception)
        {
            if (exception.Number != SqlError.UNIQUENESS_VIOLATION && exception.Number != SqlError.DEADLOCK_VICTIM) throw;

            conn.Dispose();
            return await GetMessages(storedId, skip);
        }

        return storedMessages;
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }

    private string? _getSuspensionStatusSql;
    private async Task<FunctionStatus?> GetSuspensionStatus(StoredId storedId, SqlConnection connection)
    {
        _getSuspensionStatusSql ??= @$"    
            SELECT Epoch, Status
            FROM {_tablePrefix}
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";
        await using var command = new SqlCommand(_getSuspensionStatusSql, connection);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var epoch = reader.GetInt32(0);
            var status = (Status) reader.GetInt32(1);
            return new FunctionStatus(status, epoch);
        }

        return null;
    }
}