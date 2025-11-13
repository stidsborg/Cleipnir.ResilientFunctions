using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerMessageStore : IMessageStore
{
    private readonly string _connectionString;
    private readonly SqlGenerator _sqlGenerator;
    private readonly string _tablePrefix;
    private readonly MessageBatcher<StoredMessage> _messageBatcher;

    public SqlServerMessageStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _sqlGenerator = sqlGenerator;
        _tablePrefix = tablePrefix;
        _messageBatcher = new MessageBatcher<StoredMessage>(AppendMessages);
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        _initializeSql ??= @$"
        CREATE TABLE {_tablePrefix}_Messages (
            Id UNIQUEIDENTIFIER,
            Position BIGINT,
            Content VARBINARY(MAX),
            PRIMARY KEY (Id, Position)
        );";
        var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_Messages;";
        var command = new SqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task AppendMessage(StoredId storedId, StoredMessage storedMessage)
        => await _messageBatcher.Handle(storedId, [storedMessage]);

    private async Task AppendMessages(StoredId storedId, IReadOnlyList<StoredMessage> messages)
    {
        if (messages.Count == 0)
            return;

        var randomOffset = Random.Shared.Next();
        var values = messages.Select((_, i) => $"(@Id, @MaxPos + {i + 1}, @Content{i})").StringJoin(", ");

        var sql = @$"
            DECLARE @MaxPos BIGINT;
            SELECT @MaxPos = COALESCE(MAX(Position), -1) + 2147483647 + @RandomOffset
            FROM {_tablePrefix}_Messages
            WHERE Id = @Id;

            INSERT INTO {_tablePrefix}_Messages (Id, Position, Content)
            VALUES {values};";

        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(sql, conn);

        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        command.Parameters.AddWithValue("@RandomOffset", (long)randomOffset);

        for (var i = 0; i < messages.Count; i++)
        {
            var (messageContent, messageType, _, idempotencyKey) = messages[i];
            var content = BinaryPacker.Pack(messageContent, messageType, idempotencyKey?.ToUtf8Bytes());
            command.Parameters.AddWithValue($"@Content{i}", content);
        }

        await command.ExecuteNonQueryAsync();

        // Execute interrupt command
        var interruptCommand = _sqlGenerator.Interrupt([storedId]);
        await using var interruptCmd = interruptCommand.ToSqlCommand(conn);
        await interruptCmd.ExecuteNonQueryAsync();
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages, bool interrupt = true)
    {
        if (messages.Count == 0)
            return;

        if (messages.Count > 300)
        {
            foreach (var chunk in messages.Chunk(300))
                await AppendMessages(chunk, interrupt);

            return;
        }

        var storedIds = messages.Select(m => m.StoredId).Distinct().ToList();
        var maxPositions = await GetMaxPositions(storedIds);

        var interuptsSql = _sqlGenerator.Interrupt(storedIds)!;

        await using var conn = await CreateConnection();
        var sql = @$"
            INSERT INTO {_tablePrefix}_Messages
                (Id, Position, Content)
            VALUES
                 {messages.Select((_, i) => $"(@Id{i}, @Position{i}, @Content{i})").StringJoin($",{Environment.NewLine}")};

            {(interrupt ? interuptsSql.Sql : string.Empty)}";

        await using var command = new SqlCommand(sql, conn);
        for (var i = 0; i < messages.Count; i++)
        {
            var (storedId, (messageContent, messageType, _, idempotencyKey)) = messages[i];
            var position = ++maxPositions[storedId];
            var content = BinaryPacker.Pack(messageContent, messageType, idempotencyKey?.ToUtf8Bytes());
            command.Parameters.AddWithValue($"@Id{i}", storedId.AsGuid);
            command.Parameters.AddWithValue($"@Position{i}", position);
            command.Parameters.AddWithValue($"@Content{i}", content);
        }

        await command.ExecuteNonQueryAsync();
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages, bool interrupt)
    {
        if (messages.Count == 0)
            return;

        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator
            .AppendMessages(messages, interrupt)!
            .ToSqlCommand(conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();

        _replaceMessageSql ??= @$"
            UPDATE {_tablePrefix}_Messages
            SET Content = @Content
            WHERE Id = @Id AND Position = @Position";

        var (messageJson, messageType, _, idempotencyKey) = storedMessage;
        var content = BinaryPacker.Pack(
            messageJson,
            messageType,
            idempotencyKey?.ToUtf8Bytes()
        );
        await using var command = new SqlCommand(_replaceMessageSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        command.Parameters.AddWithValue("@Position", position);
        command.Parameters.AddWithValue("@Content", content);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task DeleteMessages(StoredId storedId, IEnumerable<long> positions)
    {
        var positionsList = positions.ToList();
        if (positionsList.Count == 0)
            return;

        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.DeleteMessages(storedId, positionsList).ToSqlCommand(conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= @$"
            DELETE FROM {_tablePrefix}_Messages
            WHERE Id = @Id;";

        await using var command = new SqlCommand(_truncateSql, conn);
        
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, long skip)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.GetMessages(storedId, skip).ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await _sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content) with { Position = m.position }).ToList();
    }

    public async Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds)
    {
        await using var conn = await CreateConnection();
        await using var cmd = _sqlGenerator.GetMessages(storedIds).ToSqlCommand(conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = new Dictionary<StoredId, List<StoredMessage>>();
        foreach (var id in messages.Keys)
        {
            storedMessages[id] = new();
            foreach (var (content, position) in messages[id])
                storedMessages[id].Add(ConvertToStoredMessage(content) with { Position = position });
        }

        return storedMessages;
    }

    public async Task<IDictionary<StoredId, long>> GetMaxPositions(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return new Dictionary<StoredId, long>();

        var sql = @$"
            SELECT Id, MAX(Position)
            FROM {_tablePrefix}_Messages
            WHERE Id IN ({storedIds.Select(id => $"'{id}'").StringJoin(", ")})
            GROUP BY Id;";

        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(sql, conn);

        var positions = new Dictionary<StoredId, long>(capacity: storedIds.Count);
        foreach (var storedId in storedIds)
            positions[storedId] = -1;

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0);
            var storedId = new StoredId(id);
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
            Position: 0,
            idempotencyKey?.ToStringFromUtf8Bytes()
        );
        return storedMessage;
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}