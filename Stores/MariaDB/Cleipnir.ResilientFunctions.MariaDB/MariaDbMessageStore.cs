using System;
using Cleipnir.ResilientFunctions.Domain;
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
                position BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                id CHAR(32),
                replica CHAR(32) NOT NULL,
                content LONGBLOB,
                INDEX {_tablePrefix}_messages_id_idx (id)
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


    public async Task<ReplicaId> AppendMessage(StoredId storedId, StoredMessage storedMessage)
    {
        var (messageContent, messageType, _, replica, idempotencyKey, sender, receiver) = storedMessage;
        var content = BinaryPacker.Pack(messageContent, messageType, idempotencyKey?.ToUtf8Bytes(), sender?.ToUtf8Bytes(), receiver?.ToUtf8Bytes());

        var sql = @$"
            INSERT INTO {_tablePrefix}_messages (id, replica, content)
            VALUES (?, COALESCE((SELECT owner FROM {_tablePrefix} WHERE id = ?), ?), ?)
            RETURNING replica;";

        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = new MySqlCommand(sql, conn);
        command.Parameters.Add(new() { Value = storedId.AsGuid.ToString("N") });
        command.Parameters.Add(new() { Value = storedId.AsGuid.ToString("N") });
        command.Parameters.Add(new() { Value = replica.AsGuid.ToString("N") });
        command.Parameters.Add(new() { Value = content });
        return ((string) (await command.ExecuteScalarAsync())!).ParseToReplicaId();
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages)
    {
        if (messages.Count == 0)
            return;

        var storedIds = messages.Select(m => m.StoredId).Distinct().ToList();

        var appendMessagesCommand = _sqlGenerator.AppendMessages(messages);
        var interruptsCommand = _sqlGenerator.Interrupt(storedIds);

        var command = StoreCommand.Merge(appendMessagesCommand, interruptsCommand);

        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var sqlCommand = command.ToSqlCommand(conn);

        await sqlCommand.ExecuteNonQueryAsync();
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var (messageJson, messageType, _, replica, idempotencyKey, sender, receiver) = storedMessage;

        _replaceMessageSql ??= @$"
                UPDATE {_tablePrefix}_messages
                SET replica = COALESCE((SELECT owner FROM {_tablePrefix} WHERE id = ?), ?), content = ?
                WHERE id = ? AND position = ?";
        var content = BinaryPacker.Pack(
            messageJson,
            messageType,
            idempotencyKey?.ToUtf8Bytes(),
            sender?.ToUtf8Bytes(),
            receiver?.ToUtf8Bytes()
        );
        await using var command = new MySqlCommand(_replaceMessageSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.AsGuid.ToString("N")},
                new() {Value = replica.AsGuid.ToString("N")},
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

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetMessages(storedId)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content, m.position, m.replica)).ToList();
    }

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions)
    {
        if (!skipPositions.Any())
            return await GetMessages(storedId);

        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetMessages(storedId, skipPositions)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content, m.position, m.replica)).ToList();
    }

    public async Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds)
    {
        storedIds = storedIds.ToList();
        
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetMessages(storedIds)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = storedIds.ToDictionary(id => id, _ => new List<StoredMessage>());
        
        foreach (var id in messages.Keys)
        foreach (var (content, position, replica) in messages[id])
            storedMessages[id].Add(ConvertToStoredMessage(content, position, replica));

        return storedMessages;
    }

    public async Task<Dictionary<StoredId, List<StoredMessage>>> GetMessagesForReplica(ReplicaId replicaId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetMessagesForReplica(replicaId)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();

        var messages = await _sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = new Dictionary<StoredId, List<StoredMessage>>();

        foreach (var id in messages.Keys)
            storedMessages[id] = messages[id]
                .Select(m => ConvertToStoredMessage(m.content, m.position, m.replica))
                .ToList();

        return storedMessages;
    }

    public async Task<List<StoredIdAndPosition>> GetCrashedReplicaMessages(IReadOnlySet<ReplicaId> liveReplicas)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .GetCrashedReplicaMessages(liveReplicas)
            .ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        return await _sqlGenerator.ReadStoredIdAndPositions(reader);
    }

    public async Task SetReplica(IEnumerable<long> positions, ReplicaId newReplica, ReplicaId expectedReplica)
    {
        var positionsList = positions.ToList();
        if (positionsList.Count == 0)
            return;

        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .SetReplica(positionsList, newReplica, expectedReplica)
            .ToSqlCommand(conn);
        await command.ExecuteNonQueryAsync();
    }

    public static StoredMessage ConvertToStoredMessage(byte[] content, long position, string? replica)
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
            replica?.ParseToReplicaId() ?? ReplicaId.Empty,
            idempotencyKey?.ToStringFromUtf8Bytes(),
            sender?.ToStringFromUtf8Bytes(),
            receiver?.ToStringFromUtf8Bytes()
        );
        return storedMessage;
    }
}