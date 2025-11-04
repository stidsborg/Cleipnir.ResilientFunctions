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
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_messages;";
        var command = new NpgsqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _appendMessageSql;
    public async Task AppendMessage(StoredId storedId, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        await using var batch = new NpgsqlBatch(conn);
        var (messageJson, messageType, _, idempotencyKey) = storedMessage;
        
        {
            _appendMessageSql ??= @$"
                INSERT INTO {_tablePrefix}_messages
                    (id, position, content)
                VALUES (
                     $1,
                     (SELECT COALESCE(MAX(position), 0) + 2147483647 + $2 FROM {_tablePrefix}_messages WHERE id = $1),
                     $3
                );";
            var content = BinaryPacker.Pack(messageJson, messageType, idempotencyKey?.ToUtf8Bytes());
            var command = new NpgsqlBatchCommand(_appendMessageSql)
            {
                Parameters =
                {
                    new() {Value = storedId.AsGuid},
                    new() {Value = Random.Shared.Next()},
                    new() {Value = content}
                }
            };
            batch.BatchCommands.Add(command);
        }

        await batch.ExecuteNonQueryAsync();
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
            await using var command = StoreCommandExtensions
                .ToNpgsqlBatch([appendMessagesCommand, interruptCommand!])
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
    public async Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        _replaceMessageSql ??= @$"    
                UPDATE {_tablePrefix}_messages
                SET content = $1
                WHERE id = $2 AND position = $3";

        var (messageJson, messageType, _, idempotencyKey) = storedMessage;
        var content = BinaryPacker.Pack(
            messageJson,
            messageType,
            idempotencyKey?.ToUtf8Bytes()
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

    private string? _truncateFunctionSql;
    public async Task Truncate(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _truncateFunctionSql ??= @$"    
                DELETE FROM {_tablePrefix}_messages
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
        return messages.Select(m => ConvertToStoredMessage(m.content) with { Position = m.position }).ToList();
    }

    public async Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetMessages(storedIds).ToNpgsqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = new Dictionary<StoredId, List<StoredMessage>>();
        foreach (var id in messages.Keys)
        {
            storedMessages[id] = new();
            foreach (var (content, position) in messages[id])
                storedMessages[id].Add(ConvertToStoredMessage(content) with { Position = position });
        }

        return storedMessages;
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
            Position: 0,
            idempotencyKey?.ToStringFromUtf8Bytes()
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
            WHERE Id IN ({storedIds.Select(id => $"'{id}'").StringJoin(", ")})
            GROUP BY id;";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(sql, conn);

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