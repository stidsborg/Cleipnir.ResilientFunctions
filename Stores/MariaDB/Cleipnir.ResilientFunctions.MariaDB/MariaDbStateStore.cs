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
                id CHAR(32),
                position BIGINT,
                content LONGBLOB,
                version INT,
                PRIMARY KEY (id, position)
            );";
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public StoreCommand Get(IReadOnlyList<StoredId> ids)
    {
        var idsClause = ids.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ");
        var sql = $@"
            SELECT id, position, content, version
            FROM {tablePrefix}_state
            WHERE id IN ({idsClause})";

        return StoreCommand.Create(sql);
    }

    public async Task<Dictionary<StoredId, Dictionary<long, StoredState>>> Read(IStoreCommandReader reader)
    {
        var result = new Dictionary<StoredId, Dictionary<long, StoredState>>();

        while (await reader.ReadAsync())
        {
            var id = new StoredId(reader.GetGuid(0));
            var position = reader.GetInt64(1);
            var content = reader.IsDbNull(2) ? null : (byte[])reader.GetValue(2);
            var version = reader.GetInt32(3);

            if (!result.ContainsKey(id))
                result[id] = new Dictionary<long, StoredState>();

            result[id][position] = new StoredState(id, position, content, version);
        }

        await reader.MoveToNextResults();
        return result;
    }

    public StoreCommand Delete(StoredId id, IReadOnlyList<long> positions)
    {
        if (positions.Count == 0)
            return StoreCommand.Create("SELECT;");

        var sql = $@"
            DELETE FROM {tablePrefix}_state
            WHERE id = '{id.AsGuid:N}' AND position IN ({positions.Select(p => p.ToString()).StringJoin(",")});";

        return StoreCommand.Create(sql);
    }

    public StoreCommand Update(StoredId id, StoredState state)
    {
        var sql = $@"
            UPDATE {tablePrefix}_state
            SET content = ?, version = version + 1
            WHERE id = ? AND position = ? AND version = ?;";

        return StoreCommand.Create(
            sql,
            [
                state.Content!,
                id.AsGuid.ToString("N"),
                state.Position,
                state.Version
            ]
        );
    }

    public StoreCommand Insert(StoredId id, StoredState state)
    {
        var sql = $@"
            INSERT INTO {tablePrefix}_state
                (id, position, content, version)
            VALUES
                (?, ?, ?, ?);";

        return StoreCommand.Create(
            sql,
            [
                id.AsGuid.ToString("N"),
                state.Position,
                state.Content!,
                state.Version
            ]
        );
    }

    public StoreCommand AddTo0(StoredId id, StoredState state)
    {
        var sql = $@"
            UPDATE {tablePrefix}_state
            SET content = CONCAT(content, ?), version = version + 1
            WHERE id = ? AND position = 0 AND version = ?;";

        return StoreCommand.Create(
            sql,
            [
                state.Content!,
                id.AsGuid.ToString("N"),
                state.Version
            ]
        );
    }

    public StoreCommand Append(StoredId id, StoredState state)
    {
        var sql = $@"
            INSERT INTO {tablePrefix}_state
                (id, position, content, version)
            VALUES
                (?, (SELECT COALESCE(MAX(position), -1) + 3 FROM {tablePrefix}_state WHERE id = ?), ?, 0);";

        return StoreCommand.Create(
            sql,
            [
                id.AsGuid.ToString("N"),
                id.AsGuid.ToString("N"),
                state.Content!
            ]
        );
    }

    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }

    public record StoredState(StoredId Id, long Position, byte[]? Content, int Version);
}
