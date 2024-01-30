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

public class PostgreSqlMessageStore : IMessageStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;
    
    public PostgreSqlMessageStore(string connectionString, string tablePrefix = "")
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
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_messages (
                function_type_id VARCHAR(255),
                function_instance_id VARCHAR(255),
                position INT NOT NULL,
                message_json TEXT NOT NULL,
                message_type VARCHAR(255) NOT NULL,   
                idempotency_key VARCHAR(255),          
                PRIMARY KEY (function_type_id, function_instance_id, position)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_{_tablePrefix}rfunctions_messages_idempotencykeys
            ON {_tablePrefix}rfunctions_messages(function_type_id, function_instance_id, idempotency_key)";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = await CreateConnection();
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_messages;";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        var sql = @$"TRUNCATE TABLE {_tablePrefix}rfunctions_messages;";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<FunctionStatus> AppendMessage(FunctionId functionId, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        await using var batch = new NpgsqlBatch(conn);
        var (messageJson, messageType, idempotencyKey) = storedMessage;
       
        { //append Message to message stream sql
            var sql = @$"    
                INSERT INTO {_tablePrefix}rfunctions_messages
                    (function_type_id, function_instance_id, position, message_json, message_type, idempotency_key)
                VALUES (
                     $1, $2, 
                     (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}rfunctions_messages WHERE function_type_id = $1 AND function_instance_id = $2), 
                     $3, $4, $5
                ) RETURNING position;";
            var command = new NpgsqlBatchCommand(sql)
            {
                Parameters =
                {
                    new() {Value = functionId.TypeId.Value},
                    new() {Value = functionId.InstanceId.Value},
                    new() {Value = messageJson},
                    new() {Value = messageType},
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
            if (e.ConstraintName?.EndsWith("_pkey") == true)
            {
                await Task.Delay(Random.Shared.Next(10, 250));
                conn.Dispose();
                return await AppendMessage(functionId, storedMessage);
            }
            //read status separately
            return await GetSuspensionStatus(functionId);
        } //ignore entry already exist error
        
        throw new ConcurrentModificationException(functionId); //row must have been deleted concurrently
    }
    
    public Task<FunctionStatus> AppendMessage(FunctionId functionId, string messageJson, string messageType, string? idempotencyKey = null)
        => AppendMessage(functionId, new StoredMessage(messageJson, messageType, idempotencyKey));
    
    public async Task<FunctionStatus> AppendMessages(FunctionId functionId, IEnumerable<StoredMessage> storedMessages)
    {
        await using var conn = await CreateConnection();
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable);
        await AppendMessages(functionId, storedMessages, conn, transaction);
        await transaction.CommitAsync();
        
        return await GetSuspensionStatus(functionId);
    }

    internal async Task AppendMessages(
        FunctionId functionId, 
        IEnumerable<StoredMessage> storedMessages,
        NpgsqlConnection connection, 
        NpgsqlTransaction? transaction)
    {
        var batch = new NpgsqlBatch(connection, transaction);
        foreach (var (messageJson, messageType, idempotencyKey) in storedMessages)
        {
            string sql;
            if (idempotencyKey == null)
                sql = @$"    
                INSERT INTO {_tablePrefix}rfunctions_messages
                    (function_type_id, function_instance_id, position, message_json, message_type, idempotency_key)
                VALUES
                    ($1, $2, (SELECT (COALESCE(MAX(position), -1) + 1) FROM {_tablePrefix}rfunctions_messages WHERE function_type_id = $1 AND function_instance_id = $2), $3, $4, $5);";
            else
                sql = @$"
                    INSERT INTO {_tablePrefix}rfunctions_messages
                    SELECT $1, $2, (SELECT (COALESCE(MAX(position), -1) + 1) FROM {_tablePrefix}rfunctions_messages WHERE function_type_id = $1 AND function_instance_id = $2), $3, $4, $5
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {_tablePrefix}rfunctions_messages
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
                    new() {Value = messageJson},
                    new() {Value = messageType},
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

    public async Task<bool> Replace(FunctionId functionId, IEnumerable<StoredMessage> storedMessages, int? expectedMessageCount)
    {
        await using var conn = await CreateConnection();
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable);

        if (expectedMessageCount != null)
        {
            var existingCount = await GetMessagesCount(functionId, conn, transaction);
            if (existingCount != expectedMessageCount)
                return false;
        }
        
        await Truncate(functionId, conn, transaction);
        await AppendMessages(functionId, storedMessages, conn, transaction);

        await transaction.CommitAsync();
        return true;
    }

    private async Task<int> GetMessagesCount(FunctionId functionId, NpgsqlConnection conn, NpgsqlTransaction transaction)
    {
        var sql = @$"    
            SELECT (COALESCE(MAX(position), -1) + 1) 
            FROM {_tablePrefix}rfunctions_messages 
            WHERE function_type_id = $1 AND function_instance_id = $2";
        
        await using var command = new NpgsqlCommand(sql, conn, transaction)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        
        var count = (int?) await command.ExecuteScalarAsync();
        ArgumentNullException.ThrowIfNull(count);
        
        return count.Value;
    }
    
    internal async Task<int> Truncate(FunctionId functionId, NpgsqlConnection connection, NpgsqlTransaction? transaction)
    {
        var sql = @$"    
                DELETE FROM {_tablePrefix}rfunctions_messages
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

    public Task<IEnumerable<StoredMessage>> GetMessages(FunctionId functionId)
        => InnerGetMessages(functionId, skip: 0).SelectAsync(messages => (IEnumerable<StoredMessage>) messages);
    
    private async Task<List<StoredMessage>> InnerGetMessages(FunctionId functionId, int skip)
    {
        await using var conn = await CreateConnection();
        var sql = @$"    
            SELECT message_json, message_type, idempotency_key
            FROM {_tablePrefix}rfunctions_messages
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