using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgresEffectsStore : IEffectsStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public PostgresEffectsStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        
        await using var conn = await CreateConnection();
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunction_effects (
                id VARCHAR(450) PRIMARY KEY,
                status INT NOT NULL,
                result TEXT NULL,
                exception TEXT NULL
            );";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        var sql = $"TRUNCATE TABLE {_tablePrefix}rfunction_effects";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task SetEffectResult(FunctionId functionId, StoredEffect storedEffect)
    {
        var (functionTypeId, functionInstanceId) = functionId;
        
        await using var conn = await CreateConnection();
        var sql = $@"
          INSERT INTO {_tablePrefix}rfunction_effects 
              (id, status, result, exception)
          VALUES
              ($1, $2, $3, $4) 
          ON CONFLICT (id) 
          DO 
            UPDATE SET status = EXCLUDED.status, result = EXCLUDED.result, exception = EXCLUDED.exception";
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(functionTypeId.Value, functionInstanceId.Value, storedEffect.EffectId.Value)},
                new() {Value = (int) storedEffect.WorkStatus},
                new() {Value = storedEffect.Result ?? (object) DBNull.Value},
                new() {Value = JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredEffect>> GetEffectResults(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT id, status, result, exception
            FROM {_tablePrefix}rfunction_effects
            WHERE id LIKE $1";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value) + $"{Escaper.Separator}%" }
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredEffect>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0);
            var effectId = Escaper.Unescape(id)[2];
            var status = (WorkStatus) reader.GetInt32(1);
            var result = reader.IsDBNull(2) ? null : reader.GetString(2);
            var exception = reader.IsDBNull(3) ? null : reader.GetString(3);
            functions.Add(new StoredEffect(effectId, status, result, JsonHelper.FromJson<StoredException>(exception)));
        }

        return functions;
    }

    public async Task DeleteEffectResult(FunctionId functionId, EffectId effectId)
    {
        await using var conn = await CreateConnection();
        var sql = $"DELETE FROM {_tablePrefix}rfunction_effects WHERE id = $1";
        
        var id = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value, effectId.Value);
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters = { new() {Value = id } }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task Remove(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = $"DELETE FROM {_tablePrefix}rfunction_effects WHERE id LIKE $1";
        
        var id = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value) + $"{Escaper.Separator}%";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters = { new() {Value = id } }
        };

        await command.ExecuteNonQueryAsync();
    }

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}