using System.Data;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlMessageStore : IMessageStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;
    
    public MySqlMessageStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix.ToLower();
    }

    public async Task Initialize()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_messages (
                function_type_id VARCHAR(255),
                function_instance_id VARCHAR(255),
                position INT NOT NULL,
                message_json TEXT NOT NULL,
                message_type VARCHAR(255) NOT NULL,   
                idempotency_key VARCHAR(255),          
                PRIMARY KEY (function_type_id, function_instance_id, position),
                UNIQUE INDEX (function_type_id, function_instance_id, idempotency_key)
            );";
        var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropUnderlyingTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_messages";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task TruncateTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var sql = @$"TRUNCATE TABLE {_tablePrefix}rfunctions_messages;";
        var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<FunctionStatus> AppendMessage(FunctionId functionId, StoredMessage storedMessage)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable);
        var (messageJson, messageType, idempotencyKey) = storedMessage;
        
        var sql = @$"    
                INSERT INTO {_tablePrefix}rfunctions_messages
                    (function_type_id, function_instance_id, position, message_json, message_type, idempotency_key)
                SELECT ?, ?, COALESCE(MAX(position), -1) + 1, ?, ?, ? 
                FROM {_tablePrefix}rfunctions_messages
                WHERE function_type_id = ? AND function_instance_id = ?";
        await using var command = new MySqlCommand(sql, conn, transaction)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = messageJson},
                new() {Value = messageType},
                new() {Value = idempotencyKey ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        try
        {
            await command.ExecuteNonQueryAsync();
            await transaction.CommitAsync();
        }
        catch (MySqlException e) when (e.Number == 1062)
        {
            //ignore duplicate idempotency key
        } 
        catch (MySqlException e) when (e.Number == 1213) //deadlock found when trying to get lock; try restarting transaction
        {
            await transaction.RollbackAsync();
            return await AppendMessage(functionId, storedMessage);
        }

        return await GetSuspensionStatus(functionId);
    }

    public Task<FunctionStatus> AppendMessage(FunctionId functionId, string messageJson, string messageType, string? idempotencyKey = null)
        => AppendMessage(functionId, new StoredMessage(messageJson, messageType, idempotencyKey));
    
    public async Task<FunctionStatus> AppendMessages(FunctionId functionId, IEnumerable<StoredMessage> storedMessages)
    {
        var existingCount = await GetNumberOfMessages(functionId);
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable);

        try
        {
            await AppendMessages(functionId, storedMessages, existingCount, conn, transaction);
            var suspensionStatus = await GetSuspensionStatus(functionId);
            await transaction.CommitAsync();
            return suspensionStatus;
        }
        catch (MySqlException e) when (e.Number == 1213)
        {
            await transaction.RollbackAsync();
            return await AppendMessages(functionId, storedMessages);
        }
    }

    internal async Task AppendMessages(
        FunctionId functionId,
        IEnumerable<StoredMessage> storedMessages,
        long existingCount,
        MySqlConnection connection,
        MySqlTransaction transaction)
    {
        var existingIdempotencyKeys = new HashSet<string>();
        storedMessages = storedMessages.ToList();
        if (storedMessages.Any(se => se.IdempotencyKey != null))
            existingIdempotencyKeys = await GetExistingIdempotencyKeys(functionId, connection, transaction);
        
        foreach (var (messageJson, messageType, idempotencyKey) in storedMessages)
        {
            if (idempotencyKey != null && existingIdempotencyKeys.Contains(idempotencyKey))
                continue;
            if (idempotencyKey != null)
                existingIdempotencyKeys.Add(idempotencyKey);
            
            var sql = @$"    
                INSERT INTO {_tablePrefix}rfunctions_messages
                    (function_type_id, function_instance_id, position, message_json, message_type, idempotency_key)
                VALUES
                    (?, ?, ?, ?, ?, ?);";
           
            await using var command = new MySqlCommand(sql, connection, transaction)
            {
                Parameters =
                {
                    new() {Value = functionId.TypeId.Value},
                    new() {Value = functionId.InstanceId.Value},
                    new() {Value = existingCount++},
                    new() {Value = messageJson},
                    new() {Value = messageType},
                    new() {Value = idempotencyKey ?? (object) DBNull.Value}
                }
            };

            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task<HashSet<string>> GetExistingIdempotencyKeys(
        FunctionId functionId,
        MySqlConnection connection, MySqlTransaction transaction)
    {
        var sql = @$"    
            SELECT idempotency_key
            FROM {_tablePrefix}rfunctions_messages
            WHERE function_type_id = ? AND function_instance_id = ? AND idempotency_key IS NOT NULL";
        await using var command = new MySqlCommand(sql, connection, transaction)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
            }
        };
        
        var idempotencyKeys = new HashSet<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var idempotencyKey = reader.GetString(0);
            idempotencyKeys.Add(idempotencyKey);
        }

        return idempotencyKeys;
    }

    public async Task<long> GetNumberOfMessages(FunctionId functionId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        return await GetNumberOfMessages(functionId, conn, transaction: null);
    }

    private async Task<long> GetNumberOfMessages(FunctionId functionId, MySqlConnection conn, MySqlTransaction? transaction)
    {
        var sql = $"SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}rfunctions_messages WHERE function_type_id = ? AND function_instance_id = ?";
        await using var command =
            transaction == null
                ? new MySqlCommand(sql, conn)
                : new MySqlCommand(sql, conn, transaction);
        
        command.Parameters.Add(new() {Value = functionId.TypeId.Value});
        command.Parameters.Add(new() {Value = functionId.InstanceId.Value});
        
        return (long) (await command.ExecuteScalarAsync())!;
    }

    public async Task Truncate(FunctionId functionId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await Truncate(functionId, conn, transaction: null);
    }

    internal async Task<int> Truncate(FunctionId functionId, MySqlConnection connection, MySqlTransaction? transaction)
    {
        var sql = @$"    
                DELETE FROM {_tablePrefix}rfunctions_messages
                WHERE function_type_id = ? AND function_instance_id = ?";
        
        await using var command =
            transaction == null
                ? new MySqlCommand(sql, connection)
                : new MySqlCommand(sql, connection, transaction);

        command.Parameters.Add(new() { Value = functionId.TypeId.Value });
        command.Parameters.Add(new() { Value = functionId.InstanceId.Value });
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows;
    }
    
    public async Task<bool> Replace(FunctionId functionId, IEnumerable<StoredMessage> storedMessages, int? expectedMessageCount)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable);

        if (expectedMessageCount != null)
        {
            var count = await GetNumberOfMessages(functionId, conn, transaction);
            if (count != expectedMessageCount)
                return false;
        }
        
        await Truncate(functionId, conn, transaction);
        await AppendMessages(functionId, storedMessages, existingCount: 0, conn, transaction);

        await transaction.CommitAsync();
        return true;
    }
    
    public Task<IEnumerable<StoredMessage>> GetMessages(FunctionId functionId)
        => InnerGetMessages(functionId, skip: 0).SelectAsync(messages => (IEnumerable<StoredMessage>)messages);
    
    private async Task<List<StoredMessage>> InnerGetMessages(FunctionId functionId, int skip)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = @$"    
            SELECT message_json, message_type, idempotency_key
            FROM {_tablePrefix}rfunctions_messages
            WHERE function_type_id = ? AND function_instance_id = ? AND position >= ?
            ORDER BY position ASC;";
        await using var command = new MySqlCommand(sql, conn)
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
        var skip = 0;
        var disposed = false;
        
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
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString); 
        var sql = @$"    
            SELECT epoch, status
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(sql, conn)
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
        
        throw new ConcurrentModificationException(functionId);
    }
}