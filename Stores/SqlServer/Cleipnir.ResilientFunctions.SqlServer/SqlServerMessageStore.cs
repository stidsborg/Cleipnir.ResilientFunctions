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

    public SqlServerMessageStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _sqlGenerator = sqlGenerator;
        _tablePrefix = tablePrefix;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        _initializeSql ??= @$"
        CREATE TABLE {_tablePrefix}_Messages (
            Position BIGINT IDENTITY(1,1) PRIMARY KEY,
            Id UNIQUEIDENTIFIER,
            Replica UNIQUEIDENTIFIER NOT NULL,
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
                 {messages.Select((_, i) => $"(@Id{i}, COALESCE((SELECT Owner FROM {_tablePrefix} WHERE Id = @Id{i}), @Replica{i}), @Content{i})").StringJoin($",{Environment.NewLine}")};";

        await using var command = new SqlCommand(sql, conn);
        for (var i = 0; i < messages.Count; i++)
        {
            var (storedId, (messageContent, messageType, _, replica, idempotencyKey, sender, receiver)) = messages[i];
            var content = BinaryPacker.Pack(messageContent, messageType, idempotencyKey?.ToUtf8Bytes(), sender?.ToUtf8Bytes(), receiver?.ToUtf8Bytes());
            command.Parameters.AddWithValue($"@Id{i}", storedId.AsGuid);
            command.Parameters.AddWithValue($"@Replica{i}", replica.AsGuid);
            command.Parameters.AddWithValue($"@Content{i}", content);
        }
        await command.ExecuteNonQueryAsync();

        // The interrupt is executed as a separate command - not merged into the insert above - so the
        // insert's locks are released before the interrupt UPDATE runs. Merging them caused lock contention
        // that deadlocked tight message-exchange loops (e.g. ping-pong) where two executing flows interrupt
        // each other (SQL Server's lock-wait is unbounded, so it hung indefinitely).
        await using var interruptCommand = interuptsSql.ToSqlCommand(conn);
        await interruptCommand.ExecuteNonQueryAsync();
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

    public async Task DeleteMessages(IReadOnlyList<long> positions)
    {
        if (positions.Count == 0)
            return;

        await using var conn = await CreateConnection();
        var sql = @$"
            DELETE FROM {_tablePrefix}_Messages
            WHERE Position IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@Positions, ','))";
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@Positions", string.Join(",", positions));

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

    public async Task<List<StoredMessages>> GetMessagesForReplica(ReplicaId replicaId, IReadOnlyList<long> ignorePositions)
    {
        await using var conn = await CreateConnection();
        await using var cmd = _sqlGenerator.GetMessagesForReplica(replicaId, ignorePositions).ToSqlCommand(conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = new List<StoredMessages>();
        foreach (var id in messages.Keys)
            storedMessages.Add(new StoredMessages(
                id,
                messages[id].Select(m => ConvertToStoredMessage(m.content, m.position, m.replica)).ToList()
            ));

        return storedMessages;
    }

    public async Task<List<StoredIdAndPosition>> GetCrashedReplicaMessages(IReadOnlySet<ReplicaId> liveReplicas)
    {
        await using var conn = await CreateConnection();
        await using var cmd = _sqlGenerator.GetCrashedReplicaMessages(liveReplicas).ToSqlCommand(conn);
        await using var reader = await cmd.ExecuteReaderAsync();

        return await _sqlGenerator.ReadStoredIdAndPositions(reader);
    }

    public async Task SetReplica(IEnumerable<long> positions, ReplicaId newReplica, ReplicaId expectedReplica)
    {
        var positionsList = positions.ToList();
        if (positionsList.Count == 0)
            return;

        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.SetReplica(positionsList, newReplica, expectedReplica).ToSqlCommand(conn);
        await command.ExecuteNonQueryAsync();
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