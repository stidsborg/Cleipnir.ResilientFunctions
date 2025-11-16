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
    private readonly MessageBatcher<StoredMessage> _messageBatcher;
    
    public MariaDbMessageStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        _sqlGenerator = sqlGenerator;
        _messageBatcher = new MessageBatcher<StoredMessage>(AppendMessages);
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


    public async Task AppendMessage(StoredId storedId, StoredMessage storedMessage) 
        => await _messageBatcher.Handle(storedId, [storedMessage]);
    
    private async Task AppendMessages(StoredId storedId, IReadOnlyList<StoredMessage> messages)
    {
        if (messages.Count == 0)
            return;

        const int maxRetries = 20;
        const int baseDelayMs = 10;
        MySqlException? lastException = null;

        for (var attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                var randomOffset = Random.Shared.Next();
                var values = messages.Select(_ => "(?, @max_pos := @max_pos + 1, ?)").StringJoin(", ");

                var sql = @$"
                    SET @max_pos = (SELECT COALESCE(MAX(position), -1) FROM {_tablePrefix}_messages WHERE id = ?) + 2147483647 + ?;
                    INSERT INTO {_tablePrefix}_messages
                        (id, position, content)
                    VALUES
                        {values};";

                await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
                await using var command = new MySqlCommand(sql, conn);

                command.Parameters.Add(new() { Value = storedId.AsGuid.ToString("N") });
                command.Parameters.Add(new() { Value = randomOffset });

                foreach (var (messageContent, messageType, _, idempotencyKey) in messages)
                {
                    command.Parameters.Add(new() { Value = storedId.AsGuid.ToString("N") });
                    var content = BinaryPacker.Pack(messageContent, messageType, idempotencyKey?.ToUtf8Bytes());
                    command.Parameters.Add(new() { Value = content });
                }

                await command.ExecuteNonQueryAsync();

                // Execute interrupt command
                var interruptCommand = _sqlGenerator.Interrupt([storedId]);
                await using var interruptCmd = interruptCommand.ToSqlCommand(conn);
                await interruptCmd.ExecuteNonQueryAsync();

                return; // Success - exit retry loop
            }
            catch (MySqlException ex) when (ex.ErrorCode == MySqlErrorCode.LockDeadlock && attempt < maxRetries)
            {
                lastException = ex;
                // Deadlock detected - retry with exponential backoff
                var delayMs = baseDelayMs * (1 << attempt) + Random.Shared.Next(0, baseDelayMs);
                await Task.Delay(delayMs);
                // Loop will retry
            }
        }

        // All retries exhausted - throw the last exception
        throw new InvalidOperationException(
            $"Failed to append message after {maxRetries} retries due to deadlocks",
            lastException
        );
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages)
    {
        if (messages.Count == 0)
            return;

        const int maxRetries = 5;
        const int baseDelayMs = 10;
        MySqlException? lastException = null;

        for (var attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
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

                var command = StoreCommand.Merge(appendMessagesCommand, interruptsCommand);

                await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
                await using var sqlCommand = command.ToSqlCommand(conn);

                await sqlCommand.ExecuteNonQueryAsync();
                return; // Success - exit retry loop
            }
            catch (MySqlException ex) when (ex.ErrorCode == MySqlErrorCode.LockDeadlock && attempt < maxRetries)
            {
                lastException = ex;
                // Deadlock detected - retry with exponential backoff
                var delayMs = baseDelayMs * (1 << attempt) + Random.Shared.Next(0, baseDelayMs);
                await Task.Delay(delayMs);
                // Loop will retry
            }
        }

        // All retries exhausted - throw the last exception
        throw new InvalidOperationException(
            $"Failed to append {messages.Count} message(s) after {maxRetries} retries due to deadlocks",
            lastException
        );
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages)
    {
        if (messages.Count == 0)
            return;

        const int maxRetries = 5;
        const int baseDelayMs = 10;
        MySqlException? lastException = null;

        for (var attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                var appendCommand = _sqlGenerator.AppendMessages(messages);
                var interruptCommand = _sqlGenerator.Interrupt(messages.Select(m => m.StoredId).Distinct());

                await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
                await using var command = StoreCommand
                    .Merge(appendCommand, interruptCommand)
                    .ToSqlCommand(conn);

                await command.ExecuteNonQueryAsync();
                return; // Success - exit retry loop
            }
            catch (MySqlException ex) when (ex.ErrorCode == MySqlErrorCode.LockDeadlock && attempt < maxRetries)
            {
                lastException = ex;
                // Deadlock detected - retry with exponential backoff
                var delayMs = baseDelayMs * (1 << attempt) + Random.Shared.Next(0, baseDelayMs);
                await Task.Delay(delayMs);
                // Loop will retry
            }
        }

        // All retries exhausted - throw the last exception
        throw new InvalidOperationException(
            $"Failed to append {messages.Count} message(s) after {maxRetries} retries due to deadlocks",
            lastException
        );
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var (messageJson, messageType, _, idempotencyKey) = storedMessage;

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

    public async Task DeleteMessages(StoredId storedId, IEnumerable<long> positions)
    {
        var positionsList = positions.ToList();
        if (positionsList.Count == 0)
            return;

        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator.DeleteMessages(storedId, positionsList).ToSqlCommand(conn);
        await command.ExecuteNonQueryAsync();
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

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, long skip)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetMessages(storedId, skip)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content, m.position)).ToList();
    }

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetMessages(storedId, skipPositions)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content, m.position)).ToList();
    }

    public async Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetMessages(storedIds)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = new Dictionary<StoredId, List<StoredMessage>>();
        foreach (var id in messages.Keys)
        {
            storedMessages[id] = new();
            foreach (var (content, position) in messages[id])
                storedMessages[id].Add(ConvertToStoredMessage(content, position));
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

    public static StoredMessage ConvertToStoredMessage(byte[] content, long position)
    {
        var arrs = BinaryPacker.Split(content, expectedPieces: 3);
        var message = arrs[0]!;
        var type = arrs[1]!;
        var idempotencyKey = arrs[2];
        var storedMessage = new StoredMessage(
            message,
            type,
            Position: position,
            idempotencyKey?.ToStringFromUtf8Bytes()
        );
        return storedMessage;
    }
}