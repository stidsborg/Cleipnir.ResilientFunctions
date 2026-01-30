using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlMessageStore : IMessageStore
{
    private readonly string tablePrefix;
    private readonly MessageBatcher<StoredMessage> messageBatcher;
    private readonly string connectionString;
    private readonly SqlGenerator sqlGenerator;

    public PostgreSqlMessageStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "")
    {
        this.tablePrefix = tablePrefix.ToLower();
        messageBatcher = new(AppendMessages);
        this.connectionString = connectionString;
        this.sqlGenerator = sqlGenerator;
    }
    
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
            CREATE TABLE IF NOT EXISTS {tablePrefix}_messages (
                id UUID,
                position BIGINT,
                content BYTEA,
                PRIMARY KEY (id, position)
            );";

        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        _truncateTableSql ??= $"TRUNCATE TABLE {tablePrefix}_messages;";
        var command = new NpgsqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task AppendMessage(StoredId storedId, StoredMessage storedMessage)
        => await messageBatcher.Handle(storedId, [storedMessage]);

    private async Task AppendMessages(StoredId storedId, IReadOnlyList<StoredMessage> messages)
    {
        if (messages.Count == 0)
            return;

        var randomOffset = Random.Shared.Next();
        var values = messages.Select((_, i) => $"($1, (SELECT pos FROM max_pos) + {i + 1}, ${i + 3})").StringJoin(", ");

        var sql = @$"
            WITH max_pos AS (
                SELECT COALESCE(MAX(position), -1) + 2147483647 + $2 AS pos
                FROM {tablePrefix}_messages
                WHERE id = $1
            )
            INSERT INTO {tablePrefix}_messages (id, position, content)
            VALUES {values};";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(sql, conn);

        command.Parameters.Add(new() { Value = storedId.AsGuid });
        command.Parameters.Add(new() { Value = (long)randomOffset });

        for (var i = 0; i < messages.Count; i++)
        {
            var (messageContent, messageType, _, idempotencyKey, sender, receiver) = messages[i];
            var content = BinaryPacker.Pack(messageContent, messageType, idempotencyKey?.ToUtf8Bytes(), sender?.ToUtf8Bytes(), receiver?.ToUtf8Bytes());
            command.Parameters.Add(new() { Value = content });
        }

        await command.ExecuteNonQueryAsync();

        // Execute interrupt command
        var interruptCommand = sqlGenerator.Interrupt([storedId]);
        await using var interruptCmd = interruptCommand.ToNpgsqlCommand(conn);
        await interruptCmd.ExecuteNonQueryAsync();
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages)
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

        await AppendMessages(messageWithPositions);
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages)
    {
        if (messages.Count == 0)
            return;

        var appendMessagesCommand = sqlGenerator.AppendMessages(messages);
        var interruptCommand = sqlGenerator.Interrupt(messages.Select(m => m.StoredId).Distinct());

        await using var conn = await CreateConnection();
        await using var command = StoreCommandExtensions
            .ToNpgsqlBatch([appendMessagesCommand, interruptCommand])
            .WithConnection(conn);

        await command.ExecuteNonQueryAsync();
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        _replaceMessageSql ??= @$"
                UPDATE {tablePrefix}_messages
                SET content = $1
                WHERE id = $2 AND position = $3";

        var (messageJson, messageType, _, idempotencyKey, sender, receiver) = storedMessage;
        var content = BinaryPacker.Pack(
            messageJson,
            messageType,
            idempotencyKey?.ToUtf8Bytes(),
            sender?.ToUtf8Bytes(),
            receiver?.ToUtf8Bytes()
        );
        var command = new NpgsqlCommand(_replaceMessageSql, conn)
        {
            Parameters =
            {
                new() {Value = content},
                new() {Value = storedId.AsGuid},
                new() {Value = position},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task DeleteMessages(StoredId storedId, IEnumerable<long> positions)
    {
        var positionsArray = positions.ToArray();
        if (positionsArray.Length == 0)
            return;

        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.DeleteMessages(storedId, positionsArray).ToNpgsqlCommand(conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateFunctionSql;
    public async Task Truncate(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _truncateFunctionSql ??= @$"
                DELETE FROM {tablePrefix}_messages
                WHERE id = $1;";
        await using var command = new NpgsqlCommand(_truncateFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.AsGuid}
            }
        };
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, long skip)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetMessages(storedId, skip).ToNpgsqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content, m.position)).ToList();
    }

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetMessages(storedId, skipPositions).ToNpgsqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content, m.position)).ToList();
    }

    public async Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds)
    {
        storedIds = storedIds.ToList();
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetMessages(storedIds).ToNpgsqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = storedIds.ToDictionary(id => id, _ => new List<StoredMessage>());
        foreach (var id in messages.Keys)
            foreach (var (content, position) in messages[id])
                storedMessages[id].Add(ConvertToStoredMessage(content, position));

        return storedMessages;
    }

    public static StoredMessage ConvertToStoredMessage(byte[] content, long position)
    {
        var arrs = BinaryPacker.Split(content, expectedPieces: 5);
        var message = arrs[0]!;
        var type = arrs[1]!;
        var idempotencyKey = arrs[2];
        var sender = arrs[3];
        var receiver = arrs[4];
        var storedMessage = new StoredMessage(
            message,
            type,
            Position: position,
            idempotencyKey?.ToStringFromUtf8Bytes(),
            sender?.ToStringFromUtf8Bytes(),
            receiver?.ToStringFromUtf8Bytes()
        );
        return storedMessage;
    }

    public async Task<IDictionary<StoredId, long>> GetMaxPositions(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return new Dictionary<StoredId, long>();

        var sql = @$"
            SELECT id, MAX(position)
            FROM {tablePrefix}_messages
            WHERE Id = ANY($1)
            GROUP BY id;";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(sql, conn);
        command.Parameters.Add(new NpgsqlParameter { Value = storedIds.Select(id => id.AsGuid).ToArray() });

        var positions = new Dictionary<StoredId, long>(capacity: storedIds.Count);
        foreach (var storedId in storedIds)
            positions[storedId] = -1;

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var storedId =  new StoredId(reader.GetGuid(0));
            var position = reader.GetInt64(1);
            positions[storedId] = position;
        }

        return positions;
    }
}