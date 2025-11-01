using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlStateStore(string connectionString, string tablePrefix)
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_state (
                id UUID,
                position BIGINT,
                content BYTEA,
                version INT,
                PRIMARY KEY (id, position)
            );";
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
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

    public async Task<Dictionary<StoredId, Dictionary<long, StoredState>>> Read(IStoreCommandReader reader)
    {
        var result = new Dictionary<StoredId, Dictionary<long, StoredState>>();
        
        while (await reader.ReadAsync())
        {
            var id = new StoredId(reader.GetGuid(0));
            var position = reader.GetInt64(1);
            var content = await reader.IsDbNullAsync(2) ? null : (byte[])reader.GetValue(2);
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
            return StoreCommand.Create("SELECT");

        var sql = $@"
            DELETE FROM {tablePrefix}_state
            WHERE id = '{id}' AND position in ({positions.Select(p => p.ToString()).StringJoin(",")});";

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
        await using var cmd = new NpgsqlCommand($"TRUNCATE TABLE {tablePrefix}_state", conn);
        await cmd.ExecuteNonQueryAsync();
    }

    public StoreCommand Update(StoredId id, StoredState state)
    {
        var sql = $@"
            UPDATE {tablePrefix}_state
            SET content = $1, version = version + 1
            WHERE id = $2 AND position = $3 AND version = $4;";

        return StoreCommand.Create(
            sql,
            [
                new(state.Content!),
                new(id.AsGuid),
                new(state.Position),
                new(state.Version)
            ]
        );
    }
    
    public StoreCommand Insert(StoredId id, StoredState state)
    {
        var sql = $@"
            INSERT INTO {tablePrefix}_state 
                (id, position, content, version)
            VALUES
                ($1, $2, $3, $4);";
        
        return StoreCommand.Create(
            sql,
            [
                new(id.AsGuid),
                new(state.Position),
                new(state.Content!),
                new(state.Version)
            ]
        );
    }
    
    public StoreCommand AddTo0(StoredId id, StoredState state)
    {
        var sql = $@"
            UPDATE {tablePrefix}_state
            SET content = content || $1, version = version + 1
            WHERE id = $2 AND position = 0 AND version = $3;";

        return StoreCommand.Create(
            sql,
            [
                new(state.Content!),
                new(id.AsGuid),
                new(state.Version)
            ]
        );
    }
    
    public StoreCommand Append(StoredId id, StoredState state)
    {
        var sql = $@"
            INSERT INTO {tablePrefix}_state
                (id, position, content, version)
            VALUES
                ($1, (SELECT COALESCE(MAX(position), 1) + 1 FROM {tablePrefix}_state WHERE id = $1), $2, 0);";

        return StoreCommand.Create(
            sql,
            [
                new(id.AsGuid),
                new(state.Content!)
            ]
        );
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
    
    public record StoredState(StoredId Id, long Position, byte[]? Content, int Version);
}