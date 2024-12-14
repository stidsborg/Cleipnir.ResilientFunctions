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
                type INT,
                instance CHAR(32),
                id_hash CHAR(32),
                is_state BIT,
                status INT NOT NULL,
                result LONGBLOB NULL,
                exception TEXT NULL,
                effect_id TEXT NOT NULL,
                PRIMARY KEY(type, instance, id_hash, is_state)
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
        await using var conn = await CreateConnection();
        _setEffectResultSql ??= $@"
          INSERT INTO {_tablePrefix}_effects 
              (type, instance, id_hash, is_state, status, result, exception, effect_id)
          VALUES
              (?, ?, ?, ?, ?, ?, ?, ?)  
           ON DUPLICATE KEY UPDATE
                status = VALUES(status), result = VALUES(result), exception = VALUES(exception)";
        
        await using var command = new MySqlCommand(_setEffectResultSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")},
                new() {Value = storedEffect.StoredEffectId.Value.ToString("N")},
                new() {Value = storedEffect.IsState},
                new() {Value = (int) storedEffect.WorkStatus},
                new() {Value = storedEffect.Result ?? (object) DBNull.Value},
                new() {Value = JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value},
                new() {Value = storedEffect.EffectId.Value}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public Task SetEffectResults(StoredId storedId, IEnumerable<StoredEffect> storedEffects)
    {
        throw new NotImplementedException();
    }

    private string? _getEffectResultsSql;
    public async Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _getEffectResultsSql ??= @$"
            SELECT id_hash, is_state, status, result, exception, effect_id
            FROM {_tablePrefix}_effects
            WHERE type = ? AND instance = ?";
        await using var command = new MySqlCommand(_getEffectResultsSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")},
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredEffect>();
        while (await reader.ReadAsync())
        {
            var idHash = reader.GetString(0);
            var isState = reader.GetBoolean(1);
            var status = (WorkStatus) reader.GetInt32(2);
            var result = reader.IsDBNull(3) ? null : (byte[]) reader.GetValue(3);
            var exception = reader.IsDBNull(4) ? null : reader.GetString(4);
            var effectId = reader.GetString(5);
            functions.Add(
                new StoredEffect(
                    effectId,
                    new StoredEffectId(Guid.Parse(idHash)),
                    isState,
                    status,
                    result,
                    StoredException: JsonHelper.FromJson<StoredException>(exception)
                )
            );
        }

        return functions;
    }

    private string? _deleteEffectResultSql;
    public async Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId, bool isState)
    {
        await using var conn = await CreateConnection();
        _deleteEffectResultSql ??= @$"
            DELETE FROM {_tablePrefix}_effects 
            WHERE type = ? AND instance = ? AND id_hash = ? AND is_state = ?";
        
        await using var command = new MySqlCommand(_deleteEffectResultSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
                new() { Value = effectId.Value.ToString("N") },
                new() { Value = isState },
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= $"DELETE FROM {_tablePrefix}_effects WHERE type = ? AND instance = ?";
        await using var command = new MySqlCommand(_removeSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
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