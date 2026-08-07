using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlTypeStore(string connectionString, string tablePrefix = "") : ITypeStore
{
    private readonly string _tablePrefix = tablePrefix.ToLower();

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_dotnet_types (
                id BIGINT PRIMARY KEY,
                type BYTEA NOT NULL
            );";
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_dotnet_types";
        var command = new NpgsqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _insertTypesSql;
    public async Task InsertTypes(IReadOnlyDictionary<TypeId, byte[]> types)
    {
        if (types.Count == 0)
            return;

        await using var conn = await CreateConnection();
        _insertTypesSql ??= @$"
            INSERT INTO {_tablePrefix}_dotnet_types (id, type)
            SELECT id, type
            FROM unnest($1::bigint[], $2::bytea[]) AS t(id, type)
            ON CONFLICT DO NOTHING;";

        await using var command = new NpgsqlCommand(_insertTypesSql, conn)
        {
            Parameters =
            {
                new() { Value = types.Keys.Select(id => id.Value).ToArray() },
                new() { Value = types.Values.ToArray() }
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyDictionary<TypeId, byte[]>> GetAllTypes()
    {
        await using var conn = await CreateConnection();
        var sql = $"SELECT id, type FROM {_tablePrefix}_dotnet_types";

        await using var command = new NpgsqlCommand(sql, conn);
        var dict = new Dictionary<TypeId, byte[]>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var id = new TypeId(reader.GetInt64(0));
            var type = reader.GetFieldValue<byte[]>(1);
            dict[id] = type;
        }

        return dict;
    }

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
