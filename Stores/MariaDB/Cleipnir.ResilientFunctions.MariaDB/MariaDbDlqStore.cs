using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbDlqStore : IDlqStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public MariaDbDlqStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_dlq (
                position BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                id CHAR(32) NOT NULL,
                content LONGBLOB NOT NULL,
                INDEX {_tablePrefix}_dlq_id_idx (id)
            );";
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_dlq;";
        var command = new MySqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Append(IReadOnlyList<StoredIdAndMessage> messages)
    {
        if (messages.Count == 0)
            return;

        var sql = @$"
            INSERT INTO {_tablePrefix}_dlq (id, content)
            VALUES {messages.Select(_ => "(?, ?)").StringJoin($",{Environment.NewLine}")};";

        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = new MySqlCommand(sql, conn);
        foreach (var (storedId, storedMessage) in messages)
        {
            var content = BinaryPacker.Pack(
                storedMessage.MessageContent,
                storedMessage.MessageType,
                storedMessage.IdempotencyKey?.ToUtf8Bytes(),
                storedMessage.Sender?.ToUtf8Bytes(),
                storedMessage.Receiver?.ToUtf8Bytes()
            );
            command.Parameters.Add(new() { Value = storedId.AsGuid.ToString("N") });
            command.Parameters.Add(new() { Value = content });
        }
        await command.ExecuteNonQueryAsync();
    }

    private string? _getAllMessagesSql;
    public async Task<IReadOnlyList<StoredDlqMessage>> GetMessages()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _getAllMessagesSql ??= @$"
            SELECT id, position, content
            FROM {_tablePrefix}_dlq
            ORDER BY position;";

        await using var command = new MySqlCommand(_getAllMessagesSql, conn);
        return await ReadDlqMessages(command);
    }

    public async Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return new List<StoredDlqMessage>();

        var sql = @$"
            SELECT id, position, content
            FROM {_tablePrefix}_dlq
            WHERE id IN ({storedIds.Select(_ => "?").StringJoin(", ")})
            ORDER BY position;";

        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        await using var command = new MySqlCommand(sql, conn);
        foreach (var storedId in storedIds)
            command.Parameters.Add(new() { Value = storedId.AsGuid.ToString("N") });

        return await ReadDlqMessages(command);
    }

    private static async Task<IReadOnlyList<StoredDlqMessage>> ReadDlqMessages(MySqlCommand command)
    {
        var messages = new List<StoredDlqMessage>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var storedId = new StoredId(Guid.Parse(reader.GetString(0)));
            var position = reader.GetInt64(1);
            var content = (byte[])reader.GetValue(2);
            var arrs = BinaryPacker.Split(content, expectedPieces: 5);
            messages.Add(new StoredDlqMessage(
                storedId,
                position,
                MessageContent: arrs[0]!,
                MessageType: arrs[1]!,
                IdempotencyKey: arrs[2]?.ToStringFromUtf8Bytes(),
                Sender: arrs[3]?.ToStringFromUtf8Bytes(),
                Receiver: arrs[4]?.ToStringFromUtf8Bytes()
            ));
        }

        return messages;
    }

    public async Task Delete(IReadOnlyList<long> positions)
    {
        if (positions.Count == 0)
            return;

        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        var sql = @$"
            DELETE FROM {_tablePrefix}_dlq
            WHERE FIND_IN_SET(position, ?) > 0";
        await using var command = new MySqlCommand(sql, conn);
        command.Parameters.Add(new() { Value = string.Join(",", positions) });

        await command.ExecuteNonQueryAsync();
    }
}
