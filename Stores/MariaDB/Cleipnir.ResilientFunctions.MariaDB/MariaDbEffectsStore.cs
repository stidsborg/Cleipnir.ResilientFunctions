using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbEffectsStore : IEffectsStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public MariaDbEffectsStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_effects (
                id VARCHAR(450),
                is_state BIT,
                status INT NOT NULL,
                result LONGBLOB NULL,
                exception TEXT NULL,
                PRIMARY KEY(id, is_state)
            );";
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_effects";
        var command = new MySqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setEffectResultSql;
    public async Task SetEffectResult(StoredId storedId, StoredEffect storedEffect)
    {
        var (flowType, flowInstance) = storedId;
        await using var conn = await CreateConnection();
        _setEffectResultSql ??= $@"
          INSERT INTO {_tablePrefix}_effects 
              (id, is_state, status, result, exception)
          VALUES
              (?, ?, ?, ?, ?)  
           ON DUPLICATE KEY UPDATE
                status = VALUES(status), result = VALUES(result), exception = VALUES(exception)";
        
        await using var command = new MySqlCommand(_setEffectResultSql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(flowType.Value.ToString(), flowInstance.Value.ToString("N"), storedEffect.EffectId.Value)},
                new() {Value = storedEffect.IsState},
                new() {Value = (int) storedEffect.WorkStatus},
                new() {Value = storedEffect.Result ?? (object) DBNull.Value},
                new() {Value = JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _getEffectResultsSql;
    public async Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _getEffectResultsSql ??= @$"
            SELECT id, is_state, status, result, exception
            FROM {_tablePrefix}_effects
            WHERE id LIKE ?";
        await using var command = new MySqlCommand(_getEffectResultsSql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(storedId.Type.Value.ToString(), storedId.Instance.Value.ToString("N")) + $"{Escaper.Separator}%" },
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredEffect>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0);
            var effectId = Escaper.Unescape(id)[2];
            var isState = reader.GetBoolean(1);
            var status = (WorkStatus) reader.GetInt32(2);
            var result = reader.IsDBNull(3) ? null : (byte[]) reader.GetValue(3);
            var exception = reader.IsDBNull(4) ? null : reader.GetString(4);
            functions.Add(
                new StoredEffect(
                    effectId,
                    isState,
                    status,
                    result,
                    JsonHelper.FromJson<StoredException>(exception)
                )
            );
        }

        return functions;
    }

    private string? _deleteEffectResultSql;
    public async Task DeleteEffectResult(StoredId storedId, EffectId effectId, bool isState)
    {
        await using var conn = await CreateConnection();
        _deleteEffectResultSql ??= $"DELETE FROM {_tablePrefix}_effects WHERE id = ? AND is_state = ?";
        var id = Escaper.Escape(storedId.Type.Value.ToString(), storedId.Instance.Value.ToString("N"), effectId.Value);
        await using var command = new MySqlCommand(_deleteEffectResultSql, conn)
        {
            Parameters =
            {
                new() { Value = id },
                new() { Value = isState },
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= $"DELETE FROM {_tablePrefix}_effects WHERE id LIKE ?";
        var id = Escaper.Escape(storedId.Type.Value.ToString(), storedId.Instance.Value.ToString("N")) + $"{Escaper.Separator}%" ;
        await using var command = new MySqlCommand(_removeSql, conn)
        {
            Parameters = { new() { Value = id } }
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