using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbTypeStore(string connectionString, string tablePrefix = "") : ITypeStore
{
    private readonly string _tablePrefix = tablePrefix.ToLower();

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_types (
                id BIGINT PRIMARY KEY,
                type BLOB NOT NULL
            )";
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_types";
        var command = new MySqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task InsertTypes(IReadOnlyDictionary<TypeId, byte[]> types)
    {
        if (types.Count == 0)
            return;

        await using var conn = await CreateConnection();
        var sql = @$"
            INSERT IGNORE INTO {_tablePrefix}_types
                (id, type)
            VALUES
                {"(?, ?)".Replicate(types.Count).StringJoin(", ")};";

        await using var command = new MySqlCommand(sql, conn);
        foreach (var (id, type) in types)
        {
            command.Parameters.Add(new MySqlParameter { Value = id.Value });
            command.Parameters.Add(new MySqlParameter { Value = type });
        }

        await command.ExecuteNonQueryAsync();
    }

    public async Task<IReadOnlyDictionary<TypeId, byte[]>> GetAllTypes()
    {
        await using var conn = await CreateConnection();
        var sql = $"SELECT id, type FROM {_tablePrefix}_types";

        await using var command = new MySqlCommand(sql, conn);
        var dict = new Dictionary<TypeId, byte[]>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var id = new TypeId(reader.GetInt64(0));
            var type = (byte[]) reader.GetValue(1);
            dict[id] = type;
        }

        return dict;
    }

    private Task<MySqlConnection> CreateConnection() => DatabaseHelper.CreateOpenConnection(connectionString);
}
