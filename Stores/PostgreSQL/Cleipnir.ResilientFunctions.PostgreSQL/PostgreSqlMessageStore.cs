using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlMessageStore : IMessageStore
{
    private readonly string tablePrefix;
    private readonly string connectionString;
    private readonly SqlGenerator sqlGenerator;

    public PostgreSqlMessageStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "")
    {
        this.tablePrefix = tablePrefix.ToLower();
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
                position BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                id UUID,
                replica UUID NULL,
                content BYTEA
            );
            CREATE INDEX IF NOT EXISTS {tablePrefix}_messages_id_idx ON {tablePrefix}_messages (id);";

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

    private string? _appendMessageSql;
    public async Task<ReplicaId> AppendMessage(StoredId storedId, StoredMessage storedMessage)
    {
        _appendMessageSql ??= @$"
            INSERT INTO {tablePrefix}_messages (id, replica, content)
            VALUES ($1, COALESCE((SELECT owner FROM {tablePrefix} WHERE id = $1), $2), $3)
            RETURNING replica;";

        var content = BinaryPacker.Pack(
            storedMessage.MessageContent,
            storedMessage.MessageType,
            storedMessage.IdempotencyKey?.ToUtf8Bytes(),
            storedMessage.Sender?.ToUtf8Bytes(),
            storedMessage.Receiver?.ToUtf8Bytes()
        );

        await using var conn = await CreateConnection();
        await using var command = StoreCommand
            .Create(_appendMessageSql, values: [storedId.AsGuid, storedMessage.Replica.AsGuid, content])
            .ToNpgsqlCommand(conn);
        var result = await command.ExecuteScalarAsync();
        return ((Guid) result!).ToReplicaId();
    }

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages)
    {
        if (messages.Count == 0)
            return;

        var commands = messages
            .GroupBy(m => m.StoredId)
            .Select(g => sqlGenerator.AppendMessages(g.Key, g.Select(m => m.StoredMessage)))
            .Append(sqlGenerator.Interrupt(messages.Select(m => m.StoredId).Distinct()));

        await using var conn = await CreateConnection();
        await using var batch = commands
            .ToNpgsqlBatch()
            .WithConnection(conn);

        await batch.ExecuteNonQueryAsync();
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        _replaceMessageSql ??= @$"
                UPDATE {tablePrefix}_messages
                SET replica = COALESCE((SELECT owner FROM {tablePrefix} WHERE id = $1), $2), content = $3
                WHERE id = $1 AND position = $4";

        var (messageJson, messageType, _, replica, idempotencyKey, sender, receiver) = storedMessage;
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
                new() {Value = storedId.AsGuid},
                new() {Value = replica.AsGuid},
                new() {Value = content},
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

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetMessages(storedId).ToNpgsqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content, m.position, m.replica)).ToList();
    }

    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetMessages(storedId, skipPositions).ToNpgsqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await sqlGenerator.ReadMessages(reader);
        return messages.Select(m => ConvertToStoredMessage(m.content, m.position, m.replica)).ToList();
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
            foreach (var (content, position, replica) in messages[id])
                storedMessages[id].Add(ConvertToStoredMessage(content, position, replica));

        return storedMessages;
    }

    public async Task<Dictionary<StoredId, List<StoredMessage>>> GetMessagesForReplica(ReplicaId replicaId)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetMessagesForReplica(replicaId).ToNpgsqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var messages = await sqlGenerator.ReadStoredIdsMessages(reader);
        var storedMessages = new Dictionary<StoredId, List<StoredMessage>>();
        foreach (var id in messages.Keys)
            storedMessages[id] = messages[id]
                .Select(m => ConvertToStoredMessage(m.content, m.position, m.replica))
                .ToList();

        return storedMessages;
    }

    public async Task<List<Tuple<StoredId, long>>> GetCrashedReplicaMessages(IEnumerable<ReplicaId> liveReplicas)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetCrashedReplicaMessages(liveReplicas).ToNpgsqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        return await sqlGenerator.ReadStoredIdAndPositions(reader);
    }

    public async Task SetReplica(IEnumerable<long> positions, ReplicaId newReplica, ReplicaId expectedReplica)
    {
        var positionsArray = positions.ToArray();
        if (positionsArray.Length == 0)
            return;

        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.SetReplica(positionsArray, newReplica, expectedReplica).ToNpgsqlCommand(conn);
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
}