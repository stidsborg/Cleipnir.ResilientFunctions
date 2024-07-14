using Cleipnir.ResilientFunctions.Domain;
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
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_messages (
                type VARCHAR(255),
                instance VARCHAR(255),
                position INT NOT NULL,
                message_json TEXT NOT NULL,
                message_type VARCHAR(255) NOT NULL,   
                idempotency_key VARCHAR(255),          
                PRIMARY KEY (type, instance, position)
            );";
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _dropUnderlyingTableSql;
    public async Task DropUnderlyingTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _dropUnderlyingTableSql ??= $"DROP TABLE IF EXISTS {_tablePrefix}_messages";
        await using var command = new MySqlCommand(_dropUnderlyingTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_messages;";
        var command = new MySqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _appendMessageSql;
    public async Task<FunctionStatus?> AppendMessage(FlowId flowId, StoredMessage storedMessage)
    {
        for (var i = 0; i < 10; i++) //retry if deadlock occurs
            try
            {
                await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
                var (messageJson, messageType, idempotencyKey) = storedMessage;
                //https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_get-lock
                var lockName = flowId.ToString().GenerateSHA256Hash();
                _appendMessageSql ??= @$"    
                    SELECT GET_LOCK(?, 10);
                    INSERT INTO {_tablePrefix}_messages
                        (type, instance, position, message_json, message_type, idempotency_key)
                    SELECT ?, ?, COALESCE(MAX(position), -1) + 1, ?, ?, ? 
                        FROM {_tablePrefix}_messages
                        WHERE type = ? AND instance = ?;
                    SELECT RELEASE_LOCK(?);

                    SELECT epoch, status
                    FROM {_tablePrefix}
                    WHERE type = ? AND instance = ?;";

                await using var command = new MySqlCommand(_appendMessageSql, conn)
                {
                    Parameters =
                    {
                        new() { Value = lockName },
                        new() { Value = flowId.Type.Value },
                        new() { Value = flowId.Instance.Value },
                        new() { Value = messageJson },
                        new() { Value = messageType },
                        new() { Value = idempotencyKey ?? (object)DBNull.Value },
                        new() { Value = flowId.Type.Value },
                        new() { Value = flowId.Instance.Value },
                        new() { Value = lockName },
                        new() { Value = flowId.Type.Value },
                        new() { Value = flowId.Instance.Value },
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

        return null;
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(FlowId flowId, int position, StoredMessage storedMessage)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var (messageJson, messageType, idempotencyKey) = storedMessage;
        
        _replaceMessageSql ??= @$"    
                UPDATE {_tablePrefix}_messages
                SET message_json = ?, message_type = ?, idempotency_key = ?
                WHERE type = ? AND instance = ? AND position = ?";
        await using var command = new MySqlCommand(_replaceMessageSql, conn)
        {
            Parameters =
            {
                new() {Value = messageJson},
                new() {Value = messageType},
                new() {Value = idempotencyKey ?? (object) DBNull.Value},
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
                new() {Value = position}
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _truncateSql;
    public async Task Truncate(FlowId flowId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _truncateSql ??= @$"    
                DELETE FROM {_tablePrefix}_messages
                WHERE type = ? AND instance = ?";
        
        await using var command = new MySqlCommand(_truncateSql, conn);
        command.Parameters.Add(new() { Value = flowId.Type.Value });
        command.Parameters.Add(new() { Value = flowId.Instance.Value });
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _getMessagesSql;
    public async Task<IReadOnlyList<StoredMessage>> GetMessages(FlowId flowId, int skip)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _getMessagesSql ??= @$"    
            SELECT message_json, message_type, idempotency_key
            FROM {_tablePrefix}_messages
            WHERE type = ? AND instance = ? AND position >= ?
            ORDER BY position ASC;";
        await using var command = new MySqlCommand(_getMessagesSql, conn)
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

    private string? _hasMoreMessagesSql;
    public async Task<bool> HasMoreMessages(FlowId flowId, int skip)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _hasMoreMessagesSql ??= @$"    
            SELECT COALESCE(MAX(position), -1)
            FROM {_tablePrefix}_messages
            WHERE type = ? AND instance = ?;";
        await using var command = new MySqlCommand(_hasMoreMessagesSql, conn)
        {
            Parameters =
            {
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value}
            }
        };

        var maxPosition = (long?) await command.ExecuteScalarAsync();
        if (maxPosition == null)
            return false;

        return maxPosition.Value + 1 > skip;
    }
}