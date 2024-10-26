using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlTypeStore(string connectionString, string tablePrefix = "") : ITypeStore
{
    private readonly string _tablePrefix = tablePrefix.ToLower();

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_types (
                ref INT AUTO_INCREMENT PRIMARY KEY,
                type VARCHAR(255),
                UNIQUE INDEX (type)
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

    public async Task<int> InsertOrGetFlowType(FlowType flowType)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
                INSERT IGNORE INTO {_tablePrefix}_types 
                    (type)
                VALUES
                    (?)";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value}
            }
        };

        await command.ExecuteNonQueryAsync();

        return await GetRef(flowType);
    }

    private async Task<int> GetRef(FlowType flowType)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(connectionString);;
        var sql = @$"    
            SELECT ref
            FROM {_tablePrefix}_types
            WHERE type = ?";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
            }
        };
        
        var value = (int?) await command.ExecuteScalarAsync();
        if (!value.HasValue)
            throw new InvalidOperationException($"Unexpected missing reference for type: '{flowType.Value}'");

        return value.Value;
    }

    public async Task<IReadOnlyDictionary<FlowType, int>> GetAllFlowTypes()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(connectionString);;
        var sql = $"SELECT type, ref FROM {_tablePrefix}_types";

        await using var command = new MySqlCommand(sql, conn);
        var dict = new Dictionary<FlowType, int>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var flowType = reader.GetString(0);
            var value = reader.GetInt32(1);
            dict[flowType] = value;
        }

        return dict;
    }

    private Task<MySqlConnection> CreateConnection() => DatabaseHelper.CreateOpenConnection(connectionString);
}