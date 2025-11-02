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

public class SqlServerMessageStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "") : IMessageStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        _initializeSql ??= @$"
        CREATE TABLE {tablePrefix}_Messages (
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
        _truncateTableSql ??= $"TRUNCATE TABLE {tablePrefix}_Messages;";
        var command = new SqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task AppendMessage(StoredId storedId, StoredMessage storedMessage)
        => await AppendMessage(storedId, storedMessage, depth: 0);

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
        
        var interuptsSql = sqlGenerator.Interrupt(storedIds)!;

        await using var conn = await CreateConnection();
        var sql = @$"
            INSERT INTO {tablePrefix}_Messages
                (Id, Position, Content)
            VALUES
                 {messages.Select((_, i) => $"(@Id{i}, @Position{i}, @Content{i})").StringJoin($",{Environment.NewLine}")};

            {(interrupt ? interuptsSql.Sql : string.Empty)}";

        await using var command = new SqlCommand(sql, conn);
        for (var i = 0; i < messages.Count; i++)
        {
            var (storedId, (messageContent, messageType, idempotencyKey)) = messages[i];
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
        await using var command = sqlGenerator
            .AppendMessages(messages, interrupt)!
            .ToSqlCommand(conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _appendMessageSql;
    private async Task AppendMessage(StoredId storedId, StoredMessage storedMessage, int depth)
    {
        await using var conn = await CreateConnection();
        
        _appendMessageSql ??= @$"
            INSERT INTO {tablePrefix}_Messages
                (Id, Position, Content)
            VALUES (
                @Id,
                (SELECT COALESCE(MAX(position), -1) + 1 FROM {tablePrefix}_Messages WHERE Id = @Id),
                @Content
            );";

        var (messageJson, messageType, idempotencyKey) = storedMessage;
        var content = BinaryPacker.Pack(messageJson, messageType, idempotencyKey?.ToUtf8Bytes());
        await using var command = new SqlCommand(_appendMessageSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        command.Parameters.AddWithValue("@Content", content);
        try
        {
            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException e)
        {
            if (depth == 10 || (e.Number != SqlError.DEADLOCK_VICTIM && e.Number != SqlError.UNIQUENESS_VIOLATION)) 
                throw;
            
            // ReSharper disable once DisposeOnUsingVariable
            await conn.DisposeAsync();
            await Task.Delay(Random.Shared.Next(50, 250));
            await AppendMessage(storedId, storedMessage, depth + 1);
        }
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        
        _replaceMessageSql ??= @$"
            UPDATE {tablePrefix}_Messages
            SET Content = @Content
            WHERE Id = @Id AND Position = @Position";

        var (messageJson, messageType, idempotencyKey) = storedMessage;
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

    private string? _truncateSql;
    public async Task Truncate(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= @$"    
            DELETE FROM {tablePrefix}_Messages
            WHERE Id = @Id;";

        await using var command = new SqlCommand(_truncateSql, conn);
        
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyList<StoredMessageWithPosition>> GetMessages(StoredId storedId, long skip)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetMessages(storedId, skip).ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await sqlGenerator.ReadMessages(reader);
        return messages.Select(m => new StoredMessageWithPosition(ConvertToStoredMessage(m.content), m.position)).ToList();
    }

    public async Task<Dictionary<StoredId, List<StoredMessageWithPosition>>> GetMessages(IEnumerable<StoredId> storedIds)
    {
        await using var conn = await CreateConnection();
        await using var cmd = sqlGenerator.GetMessages(storedIds).ToSqlCommand(conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        var messages = await sqlGenerator.ReadStoredIdsMessages(reader);
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
        if (storedIds.Count == 0)
            return new Dictionary<StoredId, long>();

        var sql = @$"
            SELECT Id, MAX(Position)
            FROM {tablePrefix}_Messages
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
            idempotencyKey?.ToStringFromUtf8Bytes()
        );
        return storedMessage;
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}