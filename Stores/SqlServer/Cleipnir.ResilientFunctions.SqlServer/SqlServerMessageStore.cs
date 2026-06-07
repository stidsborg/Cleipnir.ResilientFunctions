using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
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
            Position BIGINT IDENTITY(1,1) PRIMARY KEY,
            Id UNIQUEIDENTIFIER,
            Replica UNIQUEIDENTIFIER NULL,
            Content VARBINARY(MAX)
        );
        CREATE INDEX {_tablePrefix}_Messages_Id ON {_tablePrefix}_Messages (Id);";
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

        var values = messages.Select((_, i) => $"(@Id, COALESCE((SELECT Owner FROM {_tablePrefix} WHERE Id = @Id), @Replica{i}), @Content{i})").StringJoin(", ");

        var sql = @$"
            INSERT INTO {_tablePrefix}_Messages (Id, Replica, Content)
            VALUES {values};";

        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(sql, conn);

        command.Parameters.AddWithValue("@Id", storedId.AsGuid);

        for (var i = 0; i < messages.Count; i++)
        {
            var (messageContent, messageType, _, replica, idempotencyKey, sender, receiver) = messages[i];
            var content = BinaryPacker.Pack(messageContent, messageType, idempotencyKey?.ToUtf8Bytes(), sender?.ToUtf8Bytes(), receiver?.ToUtf8Bytes());
            command.Parameters.AddWithValue($"@Replica{i}", replica.AsGuid);
            command.Parameters.AddWithValue($"@Content{i}", content);
        }

        await command.ExecuteNonQueryAsync();

        // Execute interrupt command
        var interruptCommand = _sqlGenerator.Interrupt([storedId]);
        await using var interruptCmd = interruptCommand.ToSqlCommand(conn);
        await interruptCmd.ExecuteNonQueryAsync();
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages)
    {
        if (messages.Count == 0)
            return;

        if (messages.Count > 300)
        {
            foreach (var chunk in messages.Chunk(300))
                await AppendMessages(chunk);

            return;
        }

        var storedIds = messages.Select(m => m.StoredId).Distinct().ToList();

        var interuptsSql = _sqlGenerator.Interrupt(storedIds)!;

        await using var conn = await CreateConnection();
        var sql = @$"
            INSERT INTO {_tablePrefix}_Messages
                (Id, Replica, Content)
            VALUES
                 {messages.Select((_, i) => $"(@Id{i}, COALESCE((SELECT Owner FROM {_tablePrefix} WHERE Id = @Id{i}), @Replica{i}), @Content{i})").StringJoin($",{Environment.NewLine}")};

            {interuptsSql.Sql}";

        await using var command = new SqlCommand(sql, conn);
        for (var i = 0; i < messages.Count; i++)
        {
            var (storedId, (messageContent, messageType, _, replica, idempotencyKey, sender, receiver)) = messages[i];
            var content = BinaryPacker.Pack(messageContent, messageType, idempotencyKey?.ToUtf8Bytes(), sender?.ToUtf8Bytes(), receiver?.ToUtf8Bytes());
            command.Parameters.AddWithValue($"@Id{i}", storedId.AsGuid);
            command.Parameters.AddWithValue($"@Replica{i}", replica.AsGuid);
            command.Parameters.AddWithValue($"@Content{i}", content);
        }
        foreach (var (value, name) in interuptsSql.Parameters)
            command.Parameters.AddWithValue(name, value);

        await command.ExecuteNonQueryAsync();
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();

        _replaceMessageSql ??= @$"
            UPDATE {_tablePrefix}_Messages
            SET Replica = COALESCE((SELECT Owner FROM {_tablePrefix} WHERE Id = @Id), @Replica), Content = @Content
            WHERE Id = @Id AND Position = @Position";

        var (messageJson, messageType, _, replica, idempotencyKey, sender, receiver) = storedMessage;
        var content = BinaryPacker.Pack(
            messageJson,
            messageType,
            idempotencyKey?.ToUtf8Bytes(),
            sender?.ToUtf8Bytes(),
            receiver?.ToUtf8Bytes()
        );
        await using var command = new SqlCommand(_replaceMessageSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        command.Parameters.AddWithValue("@Position", position);
        command.Parameters.AddWithValue("@Replica", replica.AsGuid);
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

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.GetMessages(storedId).ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await _sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content, m.position, m.replica)).ToList();
    }

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions)
    {
        if (!skipPositions.Any())
            return await GetMessages(storedId);

        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.GetMessages(storedId, skipPositions).ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await _sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content, m.position, m.replica)).ToList();
    }

    public async Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds)
    {
        storedIds = storedIds.ToList();
        
        await using var conn = await CreateConnection();
        await using var cmd = _sqlGenerator.GetMessages(storedIds).ToSqlCommand(conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = storedIds.ToDictionary(id => id, _ => new List<StoredMessage>());
        foreach (var id in messages.Keys)
            foreach (var (content, position, replica) in messages[id])
                storedMessages[id].Add(ConvertToStoredMessage(content, position, replica));

        return storedMessages;
    }

    public async Task<Dictionary<StoredId, List<StoredMessage>>> GetMessagesForReplica(ReplicaId replicaId)
    {
        await using var conn = await CreateConnection();
        await using var cmd = _sqlGenerator.GetMessagesForReplica(replicaId).ToSqlCommand(conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = new Dictionary<StoredId, List<StoredMessage>>();
        foreach (var id in messages.Keys)
            storedMessages[id] = messages[id]
                .Select(m => ConvertToStoredMessage(m.content, m.position, m.replica))
                .ToList();

        return storedMessages;
    }

    public static StoredMessage ConvertToStoredMessage(byte[] content, long position, Guid? replica)
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
            position,
            replica?.ToReplicaId() ?? ReplicaId.Empty,
            idempotencyKey?.ToStringFromUtf8Bytes(),
            sender?.ToStringFromUtf8Bytes(),
            receiver?.ToStringFromUtf8Bytes()
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