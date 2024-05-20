using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerEffectsStore : IEffectsStore
{
    private readonly string _tablePrefix;
    private readonly Func<Task<SqlConnection>> _connFunc;

    public SqlServerEffectsStore(string connectionString, string tablePrefix = "")
    {
        _tablePrefix = tablePrefix;
        _connFunc = CreateConnection(connectionString);
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        _initializeSql ??= @$"    
            CREATE TABLE {_tablePrefix}_Effects (
                Id NVARCHAR(450) PRIMARY KEY,
                Status INT NOT NULL,
                Result NVARCHAR(MAX),
                Exception NVARCHAR(MAX)
            );";

        await using var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await _connFunc();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_Effects";
        await using var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setEffectResultSql;
    public async Task SetEffectResult(FunctionId functionId, StoredEffect storedEffect)
    {
        var (functionTypeId, functionInstanceId) = functionId;
        await using var conn = await _connFunc();
        _setEffectResultSql ??= $@"
            MERGE INTO {_tablePrefix}_Effects
                USING (VALUES (@Id,@Status,@Result,@Exception)) 
                AS source (Id,Status,Result,Exception)
                ON {_tablePrefix}_Effects.Id = source.Id
                WHEN MATCHED THEN
                    UPDATE SET Status = source.Status, Result = source.Result, Exception = source.Exception 
                WHEN NOT MATCHED THEN
                    INSERT (Id, Status, Result, Exception)
                    VALUES (source.Id, source.Status, source.Result, source.Exception);";
        
        await using var command = new SqlCommand(_setEffectResultSql, conn);
        var escapedId = Escaper.Escape(functionTypeId.ToString(), functionInstanceId.ToString(), storedEffect.EffectId.ToString());    
        command.Parameters.AddWithValue("@Id", escapedId);
        command.Parameters.AddWithValue("@Status", storedEffect.WorkStatus);
        command.Parameters.AddWithValue("@Result", storedEffect.Result ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Exception", JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    private string? _getEffectResultsSql;
    public async Task<IEnumerable<StoredEffect>> GetEffectResults(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        _getEffectResultsSql ??= @$"
            SELECT Id, Status, Result, Exception
            FROM {_tablePrefix}_Effects
            WHERE Id LIKE @IdPrefix";

        var idPrefix = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value) + $"{Escaper.Separator}%";
        await using var command = new SqlCommand(_getEffectResultsSql, conn);
        command.Parameters.AddWithValue("@IdPrefix", idPrefix);

        var storedEffects = new List<StoredEffect>();
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows && reader.Read())
        {
            var id = reader.GetString(0);
            var effectId = Escaper.Unescape(id)[2];
            var status = (WorkStatus) reader.GetInt32(1);
            var result = reader.IsDBNull(2) ? default : reader.GetString(2);
            var exception = reader.IsDBNull(3) ? default : reader.GetString(3);

            var storedException = exception == null ? null : JsonSerializer.Deserialize<StoredException>(exception);
            var storedEffect = new StoredEffect(effectId, status, result, storedException);
            storedEffects.Add(storedEffect);
        }

        return storedEffects;
    }

    private string? _deleteEffectResultSql;
    public async Task DeleteEffectResult(FunctionId functionId, EffectId effectId)
    {
        await using var conn = await _connFunc();
        _deleteEffectResultSql ??= @$"
            DELETE FROM {_tablePrefix}_Effects
            WHERE Id = @Id";

        var id = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value, effectId.Value);
        await using var command = new SqlCommand(_deleteEffectResultSql, conn);
        command.Parameters.AddWithValue("@Id", id);
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        _removeSql ??= @$"
            DELETE FROM {_tablePrefix}_Effects
            WHERE Id LIKE @Id";

        var id = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value) + $"{Escaper.Separator}%" ;
        await using var command = new SqlCommand(_removeSql, conn);
        command.Parameters.AddWithValue("@Id", id);
        
        await command.ExecuteNonQueryAsync();
    }

    private static Func<Task<SqlConnection>> CreateConnection(string connectionString)
    {
        return async () =>
        {
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return connection;
        };
    }
}