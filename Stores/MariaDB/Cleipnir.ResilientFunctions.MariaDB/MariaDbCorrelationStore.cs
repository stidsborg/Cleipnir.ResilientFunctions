using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbCorrelationStore : ICorrelationStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public MariaDbCorrelationStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initialize;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initialize ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_correlations (
                type INT NOT NULL,
                instance CHAR(32) NOT NULL,
                correlation VARCHAR(200) NOT NULL,
                PRIMARY KEY (type, instance, correlation),
                INDEX (correlation, type, instance)
            );";
        var command = new MySqlCommand(_initialize, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_correlations";
        var command = new MySqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setCorrelationSql;
    public async Task SetCorrelation(StoredId storedId, string correlationId)
    {
        var (flowType, flowInstance) = storedId;
        await using var conn = await CreateConnection();
        _setCorrelationSql ??= $@"
          INSERT IGNORE INTO {_tablePrefix}_correlations 
              (type, instance, correlation)
          VALUES
              (?, ?, ?)";
        
        await using var command = new MySqlCommand(_setCorrelationSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
                new() {Value = flowInstance.Value.ToString("N")},
                new() {Value = correlationId},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _getCorrelationsSql;
    public async Task<IReadOnlyList<StoredId>> GetCorrelations(string correlationId)
    {
        await using var conn = await CreateConnection();
        _getCorrelationsSql ??= @$"
            SELECT type, instance
            FROM {_tablePrefix}_correlations
            WHERE correlation = ?";
        await using var command = new MySqlCommand(_getCorrelationsSql, conn)
        {
            Parameters =
            {
                new() { Value = correlationId },
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var states = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var type = reader.GetInt32(0);
            var instance = reader.GetString(1).ToGuid().ToStoredInstance();
            states.Add(new StoredId(instance));
        }

        return states;
    }

    private string? _getInstancesForFlowTypeAndCorrelation;
    public async Task<IReadOnlyList<StoredInstance>> GetCorrelations(StoredType storedType, string correlationId)
    {
        await using var conn = await CreateConnection();
        _getInstancesForFlowTypeAndCorrelation ??= @$"
            SELECT instance
            FROM {_tablePrefix}_correlations
            WHERE type = ? AND correlation = ?";
        await using var command = new MySqlCommand(_getInstancesForFlowTypeAndCorrelation, conn)
        {
            Parameters =
            {
                new() { Value = storedType.Value },
                new() { Value = correlationId },
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var instances = new List<StoredInstance>();
        while (await reader.ReadAsync())
        {
            var instance = reader.GetString(0).ToGuid().ToStoredInstance();
            instances.Add(instance);
        }

        return instances;
    }

    private string? _getCorrelationsForFunctionSql;
    public async Task<IReadOnlyList<string>> GetCorrelations(StoredId storedId)
    {
        var (typeId, instanceId) = storedId;
        await using var conn = await CreateConnection();
        _getCorrelationsForFunctionSql ??= @$"
            SELECT correlation
            FROM {_tablePrefix}_correlations
            WHERE type = ? AND instance = ?";
        await using var command = new MySqlCommand(_getCorrelationsForFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = typeId.Value },
                new() { Value = instanceId.Value.ToString("N") },
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var correlations = new List<string>();
        while (await reader.ReadAsync())
        {
            var correlation = reader.GetString(0);
            correlations.Add(correlation);
        }

        return correlations;
    }

    private string? _removeCorrelationsSql;
    public async Task RemoveCorrelations(StoredId storedId)
    {
        var (typeId, instanceId) = storedId;
        await using var conn = await CreateConnection();
        _removeCorrelationsSql ??= $"DELETE FROM {_tablePrefix}_correlations WHERE type = ? AND instance = ?";
        await using var command = new MySqlCommand(_removeCorrelationsSql, conn)
        {
            Parameters =
            {
                new() { Value =  typeId.Value },
                new() { Value =  instanceId.Value.ToString("N") },
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeCorrelationSql;
    public async Task RemoveCorrelation(StoredId storedId, string correlationId)
    {
        var (typeId, instanceId) = storedId;
        await using var conn = await CreateConnection();
        _removeCorrelationSql ??= $"DELETE FROM {_tablePrefix}_correlations WHERE type = ? AND instance = ? AND correlation = ?";
        await using var command = new MySqlCommand(_removeCorrelationSql, conn)
        {
            Parameters =
            {
                new() { Value =  typeId.Value },
                new() { Value =  instanceId.Value.ToString("N") },
                new() { Value =  correlationId },
            }
        };

        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}