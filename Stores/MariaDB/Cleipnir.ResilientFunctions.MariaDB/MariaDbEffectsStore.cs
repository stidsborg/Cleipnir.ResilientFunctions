using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbEffectsStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "") : IEffectsStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_effects (
                type INT,
                instance CHAR(32),
                id_hash CHAR(32),               
                status INT NOT NULL,
                result LONGBLOB NULL,
                exception TEXT NULL,
                effect_id TEXT NOT NULL,
                PRIMARY KEY(type, instance, id_hash)
            );";
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_effects";
        var command = new MySqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setEffectResultSql;
    public async Task SetEffectResult(StoredId storedId, StoredEffect storedEffect)
    {
        await using var conn = await CreateConnection();
        _setEffectResultSql ??= $@"
          INSERT INTO {tablePrefix}_effects 
              (type, instance, id_hash, status, result, exception, effect_id)
          VALUES
              (?, ?, ?, ?, ?, ?, ?)  
           ON DUPLICATE KEY UPDATE
                status = VALUES(status), result = VALUES(result), exception = VALUES(exception)";
        
        await using var command = new MySqlCommand(_setEffectResultSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value.ToString("N")},
                new() {Value = storedEffect.StoredEffectId.Value.ToString("N")},
                new() {Value = (int) storedEffect.WorkStatus},
                new() {Value = storedEffect.Result ?? (object) DBNull.Value},
                new() {Value = JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value},
                new() {Value = storedEffect.EffectId.Serialize()}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes)
    {
        await using var conn = await CreateConnection();
        await using var command = new MySqlCommand();
        command.Connection = conn;

        var sql = sqlGenerator.UpdateEffects(command, changes);
        command.CommandText = sql;
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _getEffectResultsSql;
    public async Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _getEffectResultsSql ??= @$"
            SELECT id_hash, status, result, exception, effect_id
            FROM {tablePrefix}_effects
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
            var status = (WorkStatus) reader.GetInt32(1);
            var result = reader.IsDBNull(2) ? null : (byte[]) reader.GetValue(2);
            var exception = reader.IsDBNull(3) ? null : reader.GetString(3);
            var effectId = reader.GetString(4);
            functions.Add(
                new StoredEffect(
                    EffectId.Deserialize(effectId),
                    new StoredEffectId(Guid.Parse(idHash)),
                    status,
                    result,
                    StoredException: JsonHelper.FromJson<StoredException>(exception)
                )
            );
        }

        return functions;
    }

    private string? _deleteEffectResultSql;
    public async Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId)
    {
        await using var conn = await CreateConnection();
        _deleteEffectResultSql ??= @$"
            DELETE FROM {tablePrefix}_effects 
            WHERE type = ? AND instance = ? AND id_hash = ?";
        
        await using var command = new MySqlCommand(_deleteEffectResultSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value.ToString("N") },
                new() { Value = effectId.Value.ToString("N") },
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task DeleteEffectResults(StoredId storedId, IReadOnlyList<StoredEffectId> effectIds)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            DELETE FROM {tablePrefix}_effects 
            WHERE type = {storedId.Type.Value} AND 
                  instance = '{storedId.Instance.Value:N}' AND 
                  id_hash IN ({effectIds.Select(id => $"'{id.Value:N}'").StringJoin(", ")});";
        
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= $"DELETE FROM {tablePrefix}_effects WHERE type = ? AND instance = ?";
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
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}