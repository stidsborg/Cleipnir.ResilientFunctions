using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbStateStore(string connectionString, string tablePrefix)
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_state (
                id CHAR(36),
                position BIGINT,
                content LONGBLOB,
                PRIMARY KEY (id, position)
            );";
        var command = new MySqlCommand(_initializeSql, conn);
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

    public async Task<Dictionary<StoredId, Dictionary<long, StoredState>>> Read(MySqlDataReader reader)
    {
        var result = new Dictionary<StoredId, Dictionary<long, StoredState>>();

        while (await reader.ReadAsync())
        {
            var idValue = reader.GetValue(0);
            var id = idValue is Guid guid
                ? new StoredId(guid)
                : new StoredId(Guid.Parse(idValue.ToString()!));
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
            INSERT INTO {tablePrefix}_state (id, position, content)
            VALUES (?, ?, ?)
            ON DUPLICATE KEY UPDATE content = VALUES(content)";

        foreach (var (storedId, positions) in values)
        {
            foreach (var (position, state) in positions)
            {
                commands.Add(StoreCommand.Create(
                    sql,
                    new List<object>
                    {
                        storedId.AsGuid.ToString(),
                        position,
                        (object?)state.Content ?? DBNull.Value
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

    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }

    public record StoredState(StoredId Id, long Position, byte[]? Content);
}
