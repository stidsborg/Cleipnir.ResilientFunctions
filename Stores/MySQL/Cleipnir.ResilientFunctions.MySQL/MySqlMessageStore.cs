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

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions_messages (
                function_type_id VARCHAR(255),
                function_instance_id VARCHAR(255),
                position INT NOT NULL,
                message_json TEXT NOT NULL,
                message_type VARCHAR(255) NOT NULL,   
                idempotency_key VARCHAR(255),          
                PRIMARY KEY (function_type_id, function_instance_id, position)
            );";
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _dropUnderlyingTableSql;
    public async Task DropUnderlyingTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _dropUnderlyingTableSql ??= $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions_messages";
        await using var command = new MySqlCommand(_dropUnderlyingTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}rfunctions_messages;";
        var command = new MySqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _appendMessageSql;
    public async Task<FunctionStatus> AppendMessage(FunctionId functionId, StoredMessage storedMessage)
    {
        for (var i = 0; i < 10; i++) //retry if deadlock is occurs
            try
            {
                await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
                var (messageJson, messageType, idempotencyKey) = storedMessage;
                //https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_get-lock
                var lockName = functionId.ToString().GenerateSHA256Hash();
                _appendMessageSql ??= @$"    
                    SELECT GET_LOCK(?, 10);
                    INSERT INTO {_tablePrefix}rfunctions_messages
                        (function_type_id, function_instance_id, position, message_json, message_type, idempotency_key)
                    SELECT ?, ?, COALESCE(MAX(position), -1) + 1, ?, ?, ? 
                        FROM {_tablePrefix}rfunctions_messages
                        WHERE function_type_id = ? AND function_instance_id = ?;
                    SELECT RELEASE_LOCK(?);

                    SELECT epoch, status
                    FROM {_tablePrefix}rfunctions
                    WHERE function_type_id = ? AND function_instance_id = ?;";

                await using var command = new MySqlCommand(_appendMessageSql, conn)
                {
                    Parameters =
                    {
                        new() { Value = lockName },
                        new() { Value = functionId.TypeId.Value },
                        new() { Value = functionId.InstanceId.Value },
                        new() { Value = messageJson },
                        new() { Value = messageType },
                        new() { Value = idempotencyKey ?? (object)DBNull.Value },
                        new() { Value = functionId.TypeId.Value },
                        new() { Value = functionId.InstanceId.Value },
                        new() { Value = lockName },
                        new() { Value = functionId.TypeId.Value },
                        new() { Value = functionId.InstanceId.Value },
                    }
                };
                
                await using var reader = await command.ExecuteReaderAsync();
                await reader.NextResultAsync();
                await reader.NextResultAsync();
                while (await reader.ReadAsync())
                {
                    var epoch = reader.GetInt32(0);
                    var status = (Status)reader.GetInt32(1);
                    return new FunctionStatus(status, epoch);
                }
            }
            catch (MySqlException e) when (e.Number == 1213) //deadlock found when trying to get lock; try restarting transaction
            {
                if (i == 9)
                    throw;

                await Task.Delay(Random.Shared.Next(10, 250));
            }
        
        throw new ConcurrentModificationException(functionId);
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(FunctionId functionId, int position, StoredMessage storedMessage)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var (messageJson, messageType, idempotencyKey) = storedMessage;
        
        _replaceMessageSql ??= @$"    
                UPDATE {_tablePrefix}rfunctions_messages
                SET message_json = ?, message_type = ?, idempotency_key = ?
                WHERE function_type_id = ? AND function_instance_id = ? AND position = ?";
        await using var command = new MySqlCommand(_replaceMessageSql, conn)
        {
            Parameters =
            {
                new() {Value = messageJson},
                new() {Value = messageType},
                new() {Value = idempotencyKey ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = position}
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _truncateSql;
    public async Task Truncate(FunctionId functionId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _truncateSql ??= @$"    
                DELETE FROM {_tablePrefix}rfunctions_messages
                WHERE function_type_id = ? AND function_instance_id = ?";
        
        await using var command = new MySqlCommand(_truncateSql, conn);
        command.Parameters.Add(new() { Value = functionId.TypeId.Value });
        command.Parameters.Add(new() { Value = functionId.InstanceId.Value });
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _getMessagesSql;
    public async Task<IReadOnlyList<StoredMessage>> GetMessages(FunctionId functionId, int skip)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _getMessagesSql ??= @$"    
            SELECT message_json, message_type, idempotency_key
            FROM {_tablePrefix}rfunctions_messages
            WHERE function_type_id = ? AND function_instance_id = ? AND position >= ?
            ORDER BY position ASC;";
        await using var command = new MySqlCommand(_getMessagesSql, conn)
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

    private string? _hasMoreMessagesSql;
    public async Task<bool> HasMoreMessages(FunctionId functionId, int skip)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _hasMoreMessagesSql ??= @$"    
            SELECT COALESCE(MAX(position), -1)
            FROM {_tablePrefix}rfunctions_messages
            WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(_hasMoreMessagesSql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };

        var maxPosition = (long?) await command.ExecuteScalarAsync();
        if (maxPosition == null)
            return false;

        return maxPosition.Value + 1 > skip;
    }
}