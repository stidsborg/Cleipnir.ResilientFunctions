using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbMessageStore : IMessageStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;
    private readonly SqlGenerator _sqlGenerator;
    
    public MariaDbMessageStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        _sqlGenerator = sqlGenerator;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_messages (
                type INT,
                instance CHAR(32),
                position INT NOT NULL,
                message_json LONGBLOB NOT NULL,
                message_type LONGBLOB NOT NULL,   
                idempotency_key VARCHAR(255),          
                PRIMARY KEY (type, instance, position)
            );";
        var command = new MySqlCommand(_initializeSql, conn);
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
    public async Task AppendMessage(StoredId storedId, StoredMessage storedMessage)
    {
        for (var i = 0; i < 10; i++) //retry if deadlock occurs
            try
            {
                await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
                var (messageJson, messageType, idempotencyKey) = storedMessage;
                //https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_get-lock
                var lockName = storedId.ToString().GenerateSHA256Hash();
                _appendMessageSql ??= @$"    
                    SELECT GET_LOCK(?, 10);
                    INSERT INTO {_tablePrefix}_messages
                        (type, instance, position, message_json, message_type, idempotency_key)
                    SELECT ?, ?, COALESCE(MAX(position), -1) + 1, ?, ?, ? 
                        FROM {_tablePrefix}_messages
                        WHERE type = ? AND instance = ?;
                    SELECT RELEASE_LOCK(?);";

                await using var command = new MySqlCommand(_appendMessageSql, conn)
                {
                    Parameters =
                    {
                        new() { Value = lockName },
                        new() { Value = storedId.Type.Value },
                        new() { Value = storedId.AsGuid.ToString("N") },
                        new() { Value = messageJson },
                        new() { Value = messageType },
                        new() { Value = idempotencyKey ?? (object)DBNull.Value },
                        new() { Value = storedId.Type.Value },
                        new() { Value = storedId.AsGuid.ToString("N") },
                        new() { Value = lockName },
                        new() { Value = storedId.Type.Value },
                        new() { Value = storedId.AsGuid.ToString("N") },
                    }
                };
                
                await command.ExecuteNonQueryAsync();
                return;
            }
            catch (MySqlException e) when (e.Number == 1213) //deadlock found when trying to get lock; try restarting transaction
            {
                if (i == 9)
                    throw;

                await Task.Delay(Random.Shared.Next(10, 250));
            }
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages, bool interrupt = true)
    {
        if (messages.Count == 0)
            return;
        
        var storedIds = messages.Select(m => m.StoredId).Distinct().ToList();
        var maxPositions = await GetMaxPositions(storedIds);
        
        var messagesWithPosition = messages.Select(msg =>
            new StoredIdAndMessageWithPosition(
                msg.StoredId,
                msg.StoredMessage,
                ++maxPositions[msg.StoredId]
            )
        ).ToList();

        var appendMessagesCommand = _sqlGenerator.AppendMessages(messagesWithPosition);
        var interruptsCommand = _sqlGenerator.Interrupt(storedIds);

        var command =
            interrupt
                ? StoreCommand.Merge(appendMessagesCommand, interruptsCommand)
                : appendMessagesCommand;
        
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var sqlCommand = command.ToSqlCommand(conn);        
        
        await sqlCommand.ExecuteNonQueryAsync();
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages, bool interrupt)
    {
        if (messages.Count == 0)
            return;
        
        var appendCommand = _sqlGenerator.AppendMessages(messages);
        var interruptCommand = interrupt
            ? _sqlGenerator.Interrupt(messages.Select(m => m.StoredId).Distinct())
            : null;
        
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = StoreCommand
            .Merge(appendCommand, interruptCommand)
            .ToSqlCommand(conn);

        await command.ExecuteNonQueryAsync();
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, int position, StoredMessage storedMessage)
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
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.AsGuid.ToString("N")},
                new() {Value = position}
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _truncateSql;
    public async Task Truncate(StoredId storedId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _truncateSql ??= @$"    
                DELETE FROM {_tablePrefix}_messages
                WHERE type = ? AND instance = ?";
        
        await using var command = new MySqlCommand(_truncateSql, conn);
        command.Parameters.Add(new() { Value = storedId.Type.Value });
        command.Parameters.Add(new() { Value = storedId.AsGuid.ToString("N") });
        
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, int skip)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetMessages(storedId, skip)
            .ToSqlCommand(conn);
        
        await using var reader = await command.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadMessages(reader);
        return messages;
    }

    public async Task<IDictionary<StoredId, int>> GetMaxPositions(IReadOnlyList<StoredId> storedIds)
    {
        var predicates = storedIds
            .GroupBy(id => id.Type.Value, id => id.AsGuid)
            .Select(g => $"type = {g.Key} AND instance IN ({g.Select(instance => $"'{instance:N}'").StringJoin(", ")})")
            .StringJoin(" OR " + Environment.NewLine);

        var sql = @$"    
            SELECT type, instance, position
            FROM {_tablePrefix}_messages
            WHERE {predicates};";

        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = new MySqlCommand(sql, conn);

        var positions = new Dictionary<StoredId, int>(capacity: storedIds.Count);
        foreach (var storedId in storedIds)
            positions[storedId] = -1;
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var guid = reader.GetGuid(1);
            var storedId = new StoredId(guid);
            var position = reader.GetInt32(2);
            positions[storedId] = position;
        }
        
        return positions;
    }
}