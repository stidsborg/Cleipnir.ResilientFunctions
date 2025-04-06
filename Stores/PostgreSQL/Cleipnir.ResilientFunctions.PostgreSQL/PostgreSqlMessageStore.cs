using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlMessageStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "") : IMessageStore
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
                type INT,
                instance UUID,
                position INT NOT NULL,
                message_json BYTEA NOT NULL,
                message_type BYTEA NOT NULL,   
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
    public async Task<FunctionStatus?> AppendMessage(StoredId storedId, StoredMessage storedMessage)
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
                );";
            var command = new NpgsqlBatchCommand(_appendMessageSql)
            {
                Parameters =
                {
                    new() {Value = storedId.Type.Value},
                    new() {Value = storedId.Instance.Value},
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
                    new() {Value = storedId.Type.Value},
                    new() {Value = storedId.Instance.Value}
                }
            };
            batch.BatchCommands.Add(command);  
        }

        try
        {
            await using var reader = await batch.ExecuteReaderAsync();
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
                return await AppendMessage(storedId, storedMessage);
            }
            //read status separately
            return await GetSuspensionStatus(storedId);
        } //ignore entry already exist error

        return null;
    }

    public async Task AppendMessageNoStatusAndInterrupt(StoredId storedId, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator
            .AppendMessage(storedId, storedMessage)
            .ToNpgsqlCommand(conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages, bool interrupt = true)
    {
        var maxPositions = await GetMaxPositions(
            storedIds: messages.Select(msg => msg.StoredId).Distinct().ToList()
        );

        var messageWithPositions = messages
            .Select(msg =>
                new StoredIdAndMessageWithPosition(
                    msg.StoredId,
                    msg.StoredMessage,
                    ++maxPositions[msg.StoredId]
                )
            ).ToList();

        await AppendMessages(messageWithPositions, interrupt);
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages, bool interrupt)
    {
        if (messages.Count == 0)
            return;
        
        var appendMessagesCommand = sqlGenerator.AppendMessages(messages);
        var interruptCommand = interrupt
            ? sqlGenerator.Interrupt(messages.Select(m => m.StoredId).Distinct())
            : null;
        
        await using var conn = await CreateConnection();
        if (interrupt)
        {
            await using var command = StoreCommands
                .CreateBatch(appendMessagesCommand, interruptCommand!)
                .WithConnection(conn);
            
            await command.ExecuteNonQueryAsync();
        }
        else
        {
            await using var command = appendMessagesCommand.ToNpgsqlCommand(conn);
            await command.ExecuteNonQueryAsync();
        }
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, int position, StoredMessage storedMessage)
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
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value},
                new() {Value = position},
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _truncateFunctionSql;
    public async Task Truncate(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _truncateFunctionSql ??= @$"    
                DELETE FROM {_tablePrefix}_messages
                WHERE type = $1 AND instance = $2;";
        await using var command = new NpgsqlCommand(_truncateFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value}
            }
        };
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, int skip)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetMessages(storedId, skip).ToNpgsqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await sqlGenerator.ReadMessages(reader);
        return messages;
    }

    public async Task<IDictionary<StoredId, int>> GetMaxPositions(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return new Dictionary<StoredId, int>();
        
        var predicates = storedIds
            .GroupBy(id => id.Type.Value, id => id.Instance.Value)
            .Select(g => $"type = {g.Key} AND instance IN ({g.Select(instance => $"'{instance}'").StringJoin(", ")})")
            .StringJoin(" OR " + Environment.NewLine);

        var sql = @$"    
            SELECT type, instance, position
            FROM {tablePrefix}_messages
            WHERE {predicates};";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(sql, conn);

        var positions = new Dictionary<StoredId, int>(capacity: storedIds.Count);
        foreach (var storedId in storedIds)
            positions[storedId] = -1;
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var type = reader.GetInt32(0).ToStoredType();
            var instance = reader.GetGuid(1).ToStoredInstance();
            var storedId = new StoredId(type, instance);
            var position = reader.GetInt32(2);
            positions[storedId] = position;
        }
        
        return positions;
    }

    private string? _getSuspensionStatusSql;
    private async Task<FunctionStatus> GetSuspensionStatus(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _getSuspensionStatusSql ??= @$"    
            SELECT epoch, status
            FROM {_tablePrefix}
            WHERE type = $1 AND instance = $2;";
        await using var command = new NpgsqlCommand(_getSuspensionStatusSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value}
            }
        };

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var epoch = reader.GetInt32(0);
            var status = (Status) reader.GetInt32(1);
            return new FunctionStatus(status, epoch);
        }

        throw UnexpectedStateException.ConcurrentModification(storedId); //row must have been deleted concurrently
    }
}