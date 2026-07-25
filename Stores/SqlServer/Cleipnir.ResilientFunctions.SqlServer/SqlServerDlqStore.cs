using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerDlqStore : IDlqStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public SqlServerDlqStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        _initializeSql ??= @$"
        CREATE TABLE {_tablePrefix}_Dlq (
            Position BIGINT IDENTITY(1,1) PRIMARY KEY,
            Id UNIQUEIDENTIFIER NOT NULL,
            Content VARBINARY(MAX) NOT NULL
        );
        CREATE INDEX {_tablePrefix}_Dlq_Id ON {_tablePrefix}_Dlq (Id);";
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
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_Dlq;";
        var command = new SqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Append(IReadOnlyList<StoredIdAndMessage> messages)
    {
        if (messages.Count == 0)
            return;

        // Bulk load instead of a parameterized multi-row INSERT - no parameter or row-count limits, so no
        // chunking. Rows are streamed in caller order, so the identity column assigns dlq positions accordingly.
        // The column mappings matter: without them SqlBulkCopy maps by ordinal, colliding with the identity
        // Position column.
        var table = new DataTable();
        table.Columns.Add("Id", typeof(Guid));
        table.Columns.Add("Content", typeof(byte[]));
        foreach (var (storedId, (messageContent, messageType, _, _, idempotencyKey, sender, receiver)) in messages)
        {
            var content = BinaryPacker.Pack(messageContent, messageType, idempotencyKey?.ToUtf8Bytes(), sender?.ToUtf8Bytes(), receiver?.ToUtf8Bytes());
            table.Rows.Add(storedId.AsGuid, content);
        }

        await using var conn = await CreateConnection();
        using var bulkCopy = new SqlBulkCopy(conn);
        bulkCopy.DestinationTableName = $"{_tablePrefix}_Dlq";
        bulkCopy.ColumnMappings.Add("Id", "Id");
        bulkCopy.ColumnMappings.Add("Content", "Content");
        await bulkCopy.WriteToServerAsync(table);
    }

    private string? _getAllMessagesSql;
    public async Task<IReadOnlyList<StoredDlqMessage>> GetMessages()
    {
        await using var conn = await CreateConnection();
        _getAllMessagesSql ??= @$"
            SELECT Id, Position, Content
            FROM {_tablePrefix}_Dlq
            ORDER BY Position;";

        await using var command = new SqlCommand(_getAllMessagesSql, conn);
        return await ReadDlqMessages(command);
    }

    public async Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return new List<StoredDlqMessage>();

        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT Id, Position, Content
            FROM {_tablePrefix}_Dlq
            WHERE Id IN ({storedIds.Select((_, i) => $"@Id{i}").StringJoin(", ")})
            ORDER BY Position;";

        await using var command = new SqlCommand(sql, conn);
        for (var i = 0; i < storedIds.Count; i++)
            command.Parameters.AddWithValue($"@Id{i}", storedIds[i].AsGuid);

        return await ReadDlqMessages(command);
    }

    private static async Task<IReadOnlyList<StoredDlqMessage>> ReadDlqMessages(SqlCommand command)
    {
        var messages = new List<StoredDlqMessage>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var storedId = new StoredId(reader.GetGuid(0));
            var position = reader.GetInt64(1);
            var content = (byte[])reader.GetValue(2);
            var storedMessage = SqlServerMessageStore.ConvertToStoredMessage(content, position, replica: null);
            messages.Add(new StoredDlqMessage(storedId, position, storedMessage));
        }

        return messages;
    }

    public async Task Delete(IReadOnlyList<long> positions)
    {
        if (positions.Count == 0)
            return;

        await using var conn = await CreateConnection();
        var sql = @$"
            DELETE FROM {_tablePrefix}_Dlq
            WHERE Position IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@Positions, ','))";
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@Positions", string.Join(",", positions));

        await command.ExecuteNonQueryAsync();
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
