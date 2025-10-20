using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerStateStore(string connectionString, string tablePrefix)
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{tablePrefix}_state') AND type in (N'U'))
            BEGIN
                CREATE TABLE {tablePrefix}_state (
                    id UNIQUEIDENTIFIER,
                    position BIGINT,
                    content VARBINARY(MAX),
                    PRIMARY KEY (id, position)
                );
            END";
        var command = new SqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<Dictionary<StoredId, Dictionary<long, StoredState>>> GetAndRead(IReadOnlyList<StoredId> ids)
    {
        await using var conn = await CreateConnection();
        var cmd = Get(ids).ToSqlCommand(conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        return await Read(reader);
    }

    public StoreCommand Get(IReadOnlyList<StoredId> ids)
    {
        var idsClause = ids.Select(id => $"'{id}'").StringJoin(", ");
        var sql = $@"
            SELECT id, position, content
            FROM {tablePrefix}_state
            WHERE id IN ({idsClause})";

        return new StoreCommand(sql, []);
    }

    public async Task<Dictionary<StoredId, Dictionary<long, StoredState>>> Read(SqlDataReader reader)
    {
        var result = new Dictionary<StoredId, Dictionary<long, StoredState>>();

        while (await reader.ReadAsync())
        {
            var id = new StoredId(reader.GetGuid(0));
            var position = reader.GetInt64(1);
            var content = reader.IsDBNull(2) ? null : (byte[])reader[2];

            if (!result.ContainsKey(id))
                result[id] = new Dictionary<long, StoredState>();

            result[id][position] = new StoredState(id, position, content);
        }

        await reader.NextResultAsync();
        return result;
    }

    public IEnumerable<StoreCommand> Set(Dictionary<StoredId, Dictionary<long, StoredState>> values)
    {
        if (values.Count == 0)
            return [];

        var commands = new List<StoreCommand>();

        var sql = $@"
            MERGE {tablePrefix}_state AS target
            USING (SELECT @id AS id, @position AS position, CAST(@content AS VARBINARY(MAX)) AS content) AS source
            ON target.id = source.id AND target.position = source.position
            WHEN MATCHED THEN
                UPDATE SET content = source.content
            WHEN NOT MATCHED THEN
                INSERT (id, position, content)
                VALUES (source.id, source.position, source.content);";

        foreach (var (storedId, positions) in values)
        {
            foreach (var (position, state) in positions)
            {
                commands.Add(StoreCommand.Create(
                    sql,
                    new List<ParameterValueAndName>
                    {
                        new("@id", storedId.AsGuid),
                        new("@position", position),
                        new("@content", (object?)state.Content ?? DBNull.Value)
                    }
                ));
            }
        }

        return commands;
    }

    public async Task SetAndExecute(Dictionary<StoredId, Dictionary<long, StoredState>> values)
    {
        if (values.Count == 0)
            return;

        await using var conn = await CreateConnection();
        foreach (var command in Set(values))
        {
            await using var cmd = command.ToSqlCommand(conn);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }

    public record StoredState(StoredId Id, long Position, byte[]? Content);
}
