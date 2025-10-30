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
             CREATE TABLE {tablePrefix}_state (
                    id UNIQUEIDENTIFIER,
                    position INT,
                    content VARBINARY(MAX),
                    version INT,
                    PRIMARY KEY (id, position)
             );";
        await using var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    public StoreCommand Get(IReadOnlyList<StoredId> ids)
    {
        var idsClause = ids.Select(id => $"'{id}'").StringJoin(", ");
        var sql = $@"
            SELECT id, position, content, version
            FROM {tablePrefix}_state
            WHERE id IN ({idsClause})";

        return StoreCommand.Create(sql);
    }

    public async Task<Dictionary<StoredId, Dictionary<int, StoredState>>> Read(IStoreCommandReader reader)
    {
        var result = new Dictionary<StoredId, Dictionary<int, StoredState>>();

        while (await reader.ReadAsync())
        {
            var id = new StoredId(reader.GetGuid(0));
            var position = reader.GetInt32(1);
            var content = reader.IsDbNull(2) ? null : (byte[])reader.GetValue(2);
            var version = reader.GetInt32(3);

            if (!result.ContainsKey(id))
                result[id] = new Dictionary<int, StoredState>();

            result[id][position] = new StoredState(id, position, content, version);
        }

        await reader.MoveToNextResults();
        return result;
    }

    public StoreCommand Delete(StoredId id, IReadOnlyList<int> positions)
    {
        if (positions.Count == 0)
            return StoreCommand.Create("SELECT;");

        var sql = $@"
            DELETE FROM {tablePrefix}_state
            WHERE id = '{id}' AND position IN ({positions.Select(p => p.ToString()).StringJoin(",")});";

        return StoreCommand.Create(sql);
    }

    public StoreCommand Delete(StoredId id)
    {
        var sql = $@"
            DELETE FROM {tablePrefix}_state
            WHERE id = '{id}';";

        return StoreCommand.Create(sql);
    }

    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        await using var cmd = new SqlCommand($"TRUNCATE TABLE {tablePrefix}_state", conn);
        await cmd.ExecuteNonQueryAsync();
    }

    public StoreCommand Update(StoredId id, StoredState state)
    {
        var sql = $@"
            UPDATE {tablePrefix}_state
            SET content = @content, version = version + 1
            WHERE id = @id AND position = @position AND version = @version;";

        return StoreCommand.Create(
            sql,
            [
                new ParameterValueAndName("@content", state.Content!),
                new ParameterValueAndName("@id", id.AsGuid),
                new ParameterValueAndName("@position", state.Position),
                new ParameterValueAndName("@version", state.Version)
            ]
        );
    }

    public StoreCommand Insert(StoredId id, StoredState state)
    {
        var sql = $@"
            INSERT INTO {tablePrefix}_state
                (id, position, content, version)
            VALUES
                (@id, @position, @content, @version);";

        return StoreCommand.Create(
            sql,
            [
                new ParameterValueAndName("@id", id.AsGuid),
                new ParameterValueAndName("@position", state.Position),
                new ParameterValueAndName("@content", state.Content!),
                new ParameterValueAndName("@version", state.Version)
            ]
        );
    }

    public StoreCommand AddTo0(StoredId id, StoredState state)
    {
        var sql = $@"
            UPDATE {tablePrefix}_state
            SET content = content + @content, version = version + 1
            WHERE id = @id AND position = 0 AND version = @version;";

        return StoreCommand.Create(
            sql,
            [
                new ParameterValueAndName("@content", state.Content!),
                new ParameterValueAndName("@id", id.AsGuid),
                new ParameterValueAndName("@version", state.Version)
            ]
        );
    }

    public StoreCommand Append(StoredId id, StoredState state)
    {
        var sql = $@"
            INSERT INTO {tablePrefix}_state
                (id, position, content, version)
            VALUES
                (@id, (SELECT COALESCE(MAX(position), -1) + 3 FROM {tablePrefix}_state WHERE id = @id), @content, 0);";

        return StoreCommand.Create(
            sql,
            [
                new ParameterValueAndName("@id", id.AsGuid),
                new ParameterValueAndName("@content", state.Content!)
            ]
        );
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }

    public record StoredState(StoredId Id, int Position, byte[]? Content, int Version);
}
