using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlMessageStore(string connectionString, string tablePrefix = "") : IMessageStore
{
    private readonly string _tablePrefix = tablePrefix.ToLower();

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_messages (
                type VARCHAR(255),
                instance VARCHAR(255),
                position INT NOT NULL,
                message_json TEXT NOT NULL,
                message_type VARCHAR(255) NOT NULL,   
                idempotency_key VARCHAR(255),          
                PRIMARY KEY (type, instance, position)
            );";
        
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_messages;";
        var command = new NpgsqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _appendMessageSql;
    private string? _getFunctionStatusInAppendMessageSql;
    public async Task<FunctionStatus?> AppendMessage(FlowId flowId, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        await using var batch = new NpgsqlBatch(conn);
        var (messageJson, messageType, idempotencyKey) = storedMessage;
       
        { //append Message to message stream sql
            _appendMessageSql ??= @$"    
                INSERT INTO {_tablePrefix}_messages
                    (type, instance, position, message_json, message_type, idempotency_key)
                VALUES (
                     $1, $2, 
                     (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}_messages WHERE type = $1 AND instance = $2), 
                     $3, $4, $5
                ) RETURNING position;";
            var command = new NpgsqlBatchCommand(_appendMessageSql)
            {
                Parameters =
                {
                    new() {Value = flowId.Type.Value},
                    new() {Value = flowId.Instance.Value},
                    new() {Value = messageJson},
                    new() {Value = messageType},
                    new() {Value = idempotencyKey ?? (object) DBNull.Value}
                }
            };
            batch.BatchCommands.Add(command);            
        }

        { //get function status
            _getFunctionStatusInAppendMessageSql ??= @$"    
            SELECT epoch, status
            FROM {_tablePrefix}
            WHERE type = $1 AND instance = $2;";
           
            var command = new NpgsqlBatchCommand(_getFunctionStatusInAppendMessageSql)
            {
                Parameters = { 
                    new() {Value = flowId.Type.Value},
                    new() {Value = flowId.Instance.Value}
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
                return await AppendMessage(flowId, storedMessage);
            }
            //read status separately
            return await GetSuspensionStatus(flowId);
        } //ignore entry already exist error

        return null;
    }
    
    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(FlowId flowId, int position, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        _replaceMessageSql ??= @$"    
                UPDATE {_tablePrefix}_messages
                SET message_json = $1, message_type = $2, idempotency_key = $3
                WHERE type = $4 AND instance = $5 AND position = $6";

        var (messageJson, messageType, idempotencyKey) = storedMessage;
        var command = new NpgsqlCommand(_replaceMessageSql, conn)
        {
            Parameters =
            {
                new() {Value = messageJson},
                new() {Value = messageType},
                new() {Value = idempotencyKey ?? (object) DBNull.Value},
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
                new() {Value = position},
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _truncateFunctionSql;
    public async Task Truncate(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _truncateFunctionSql ??= @$"    
                DELETE FROM {_tablePrefix}_messages
                WHERE type = $1 AND instance = $2;";
        await using var command = new NpgsqlCommand(_truncateFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value}
            }
        };
        await command.ExecuteNonQueryAsync();
    }

    private string? _getMessagesSql;
    public async Task<IReadOnlyList<StoredMessage>> GetMessages(FlowId flowId, int skip)
    {
        await using var conn = await CreateConnection();
        _getMessagesSql ??= @$"    
            SELECT message_json, message_type, idempotency_key
            FROM {_tablePrefix}_messages
            WHERE type = $1 AND instance = $2 AND position >= $3
            ORDER BY position ASC;";
        await using var command = new NpgsqlCommand(_getMessagesSql, conn)
        {
            Parameters =
            {
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
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

    private string? _getSuspensionStatusSql;
    private async Task<FunctionStatus> GetSuspensionStatus(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _getSuspensionStatusSql ??= @$"    
            SELECT epoch, status
            FROM {_tablePrefix}
            WHERE type = $1 AND instance = $2;";
        await using var command = new NpgsqlCommand(_getSuspensionStatusSql, conn)
        {
            Parameters = { 
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value}
            }
        };

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var epoch = reader.GetInt32(0);
            var status = (Status) reader.GetInt32(1);
            return new FunctionStatus(status, epoch);
        }
        
        throw UnexpectedStateException.ConcurrentModification(flowId); //row must have been deleted concurrently
    }
}