using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlDlqStore : IDlqStore
{
    private readonly string _tablePrefix;
    private readonly string _connectionString;

    public PostgreSqlDlqStore(string connectionString, string tablePrefix = "")
    {
        _tablePrefix = tablePrefix.ToLower();
        _connectionString = connectionString;
    }

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_dlq (
                position BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                id UUID NOT NULL,
                previous_position BIGINT NOT NULL,
                content BYTEA NOT NULL
            );
            CREATE INDEX IF NOT EXISTS {_tablePrefix}_dlq_id_idx ON {_tablePrefix}_dlq (id);";

        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_dlq;";
        var command = new NpgsqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _appendSql;
    public async Task Append(IReadOnlyList<StoredIdAndMessage> messages)
    {
        if (messages.Count == 0)
            return;

        // The identity column assigns the dlq position. unnest WITH ORDINALITY expands the parameter arrays
        // into rows in parallel; ORDER BY ord makes the identity assignment follow caller order.
        _appendSql ??= @$"
            INSERT INTO {_tablePrefix}_dlq (id, previous_position, content)
            SELECT id, previous_position, content
            FROM unnest($1::uuid[], $2::bigint[], $3::bytea[]) WITH ORDINALITY AS t(id, previous_position, content, ord)
            ORDER BY ord;";

        var ids = messages.Select(m => m.StoredId.AsGuid).ToArray();
        var previousPositions = messages.Select(m => m.StoredMessage.Position).ToArray();
        var contents = messages
            .Select(m => BinaryPacker.Pack(
                m.StoredMessage.MessageContent,
                m.StoredMessage.MessageType,
                m.StoredMessage.IdempotencyKey?.ToUtf8Bytes(),
                m.StoredMessage.Sender?.ToUtf8Bytes(),
                m.StoredMessage.Receiver?.ToUtf8Bytes()
            ))
            .ToArray();

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(_appendSql, conn)
        {
            Parameters =
            {
                new() { Value = ids },
                new() { Value = previousPositions },
                new() { Value = contents }
            }
        };
        await command.ExecuteNonQueryAsync();
    }

    private string? _getAllMessagesSql;
    public async Task<IReadOnlyList<StoredDlqMessage>> GetMessages()
    {
        await using var conn = await CreateConnection();
        _getAllMessagesSql ??= @$"
            SELECT id, position, previous_position, content
            FROM {_tablePrefix}_dlq
            ORDER BY position;";

        await using var command = new NpgsqlCommand(_getAllMessagesSql, conn);
        return await ReadDlqMessages(command);
    }

    private string? _getMessagesSql;
    public async Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return new List<StoredDlqMessage>();

        await using var conn = await CreateConnection();
        _getMessagesSql ??= @$"
            SELECT id, position, previous_position, content
            FROM {_tablePrefix}_dlq
            WHERE id = ANY($1)
            ORDER BY position;";

        await using var command = new NpgsqlCommand(_getMessagesSql, conn)
        {
            Parameters =
            {
                new() { Value = storedIds.Select(id => id.AsGuid).ToArray() }
            }
        };
        return await ReadDlqMessages(command);
    }

    private static async Task<IReadOnlyList<StoredDlqMessage>> ReadDlqMessages(NpgsqlCommand command)
    {
        var messages = new List<StoredDlqMessage>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var storedId = new StoredId(reader.GetGuid(0));
            var position = reader.GetInt64(1);
            var previousPosition = reader.GetInt64(2);
            var content = (byte[])reader.GetValue(3);
            var storedMessage = PostgreSqlMessageStore.ConvertToStoredMessage(content, previousPosition, replica: null);
            messages.Add(new StoredDlqMessage(storedId, position, storedMessage));
        }

        return messages;
    }

    private string? _deleteSql;
    public async Task Delete(IReadOnlyList<long> positions)
    {
        if (positions.Count == 0)
            return;

        await using var conn = await CreateConnection();
        _deleteSql ??= @$"
            DELETE FROM {_tablePrefix}_dlq
            WHERE position = ANY($1)";
        await using var command = new NpgsqlCommand(_deleteSql, conn)
        {
            Parameters =
            {
                new() { Value = positions.ToArray() }
            }
        };
        await command.ExecuteNonQueryAsync();
    }
}
