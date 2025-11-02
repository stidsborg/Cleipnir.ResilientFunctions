using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.MariaDB.StoreCommand;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
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
                id CHAR(32),
                position BIGINT,
                content LONGBLOB,
                PRIMARY KEY (id, position)
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
                        (id, position, content)
                    SELECT ?, COALESCE(MAX(position), -1) + 1, ?
                        FROM {_tablePrefix}_messages
                        WHERE id = ?;
                    SELECT RELEASE_LOCK(?);";

                var content = BinaryPacker.Pack(messageJson, messageType, idempotencyKey?.ToUtf8Bytes());
                await using var command = new MySqlCommand(_appendMessageSql, conn)
                {
                    Parameters =
                    {
                        new() { Value = lockName },
                        new() { Value = storedId.AsGuid.ToString("N") },
                        new() { Value = content },
                        new() { Value = storedId.AsGuid.ToString("N") },
                        new() { Value = lockName },
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
    public async Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var (messageJson, messageType, idempotencyKey) = storedMessage;

        _replaceMessageSql ??= @$"
                UPDATE {_tablePrefix}_messages
                SET content = ?
                WHERE id = ? AND position = ?";
        var content = BinaryPacker.Pack(
            messageJson,
            messageType,
            idempotencyKey?.ToUtf8Bytes()
        );
        await using var command = new MySqlCommand(_replaceMessageSql, conn)
        {
            Parameters =
            {
                new() {Value = content},
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
                WHERE id = ?";
        
        await using var command = new MySqlCommand(_truncateSql, conn);
        command.Parameters.Add(new() { Value = storedId.AsGuid.ToString("N") });
        
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyList<StoredMessageWithPosition>> GetMessages(StoredId storedId, long skip)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetMessages(storedId, skip)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadMessages(reader);
        return messages.Select(m => new StoredMessageWithPosition(ConvertToStoredMessage(m.content), m.position)).ToList();
    }

    public async Task<Dictionary<StoredId, List<StoredMessageWithPosition>>> GetMessages(IEnumerable<StoredId> storedIds)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetMessages(storedIds)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = new Dictionary<StoredId, List<StoredMessageWithPosition>>();
        foreach (var id in messages.Keys)
        {
            storedMessages[id] = new();
            foreach (var (content, position) in messages[id])
                storedMessages[id].Add(new StoredMessageWithPosition(ConvertToStoredMessage(content), position));
        }

        return storedMessages;
    }

    public async Task<IDictionary<StoredId, long>> GetMaxPositions(IReadOnlyList<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, MAX(position)
            FROM {_tablePrefix}_messages
            WHERE Id IN ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")})
            GROUP BY id;";

        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = new MySqlCommand(sql, conn);

        var positions = new Dictionary<StoredId, long>(capacity: storedIds.Count);
        foreach (var storedId in storedIds)
            positions[storedId] = -1;

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var storedId = new StoredId(reader.GetString(0).ToGuid());
            var position = reader.GetInt64(1);
            positions[storedId] = position;
        }

        return positions;
    }

    public static StoredMessage ConvertToStoredMessage(byte[] content)
    {
        var arrs = BinaryPacker.Split(content, expectedPieces: 3);
        var message = arrs[0]!;
        var type = arrs[1]!;
        var idempotencyKey = arrs[2];
        var storedMessage = new StoredMessage(
            message,
            type,
            idempotencyKey?.ToStringFromUtf8Bytes()
        );
        return storedMessage;
    }
}