using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerEffectsStore(string connectionString, string tablePrefix = "") : IEffectsStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"    
            CREATE TABLE {tablePrefix}_Effects (
                FlowType INT,
                FlowInstance UNIQUEIDENTIFIER,
                StoredId UNIQUEIDENTIFIER,
                IsState BIT,
                EffectId VARCHAR(MAX) NOT NULL,                
                Status INT NOT NULL,
                Result VARBINARY(MAX),
                Exception NVARCHAR(MAX),
                
                PRIMARY KEY (FlowType, FlowInstance, StoredId, IsState)
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
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_Effects";
        await using var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setEffectResultSql;
    public async Task SetEffectResult(StoredId storedId, StoredEffect storedEffect)
    {
        await using var conn = await CreateConnection();
        _setEffectResultSql ??= $@"
            MERGE INTO {tablePrefix}_Effects
                USING (VALUES (@FlowType, @FlowInstance, @StoredId, @IsState, @EffectId, @Status, @Result, @Exception)) 
                AS source (FlowType, FlowInstance, StoredId, IsState, EffectId, Status, Result, Exception)
                ON {tablePrefix}_Effects.FlowType = source.FlowType AND {tablePrefix}_Effects.FlowInstance = source.FlowInstance AND {tablePrefix}_Effects.StoredId = source.StoredId AND {tablePrefix}_Effects.IsState = source.IsState 
                WHEN MATCHED THEN
                    UPDATE SET Status = source.Status, Result = source.Result, Exception = source.Exception 
                WHEN NOT MATCHED THEN
                    INSERT (FlowType, FlowInstance, StoredId, IsState, EffectId, Status, Result, Exception)
                    VALUES (source.FlowType, source.FlowInstance, source.StoredId, source.IsState, source.EffectId, source.Status, source.Result, source.Exception);";
        
        await using var command = new SqlCommand(_setEffectResultSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@StoredId", storedEffect.StoredEffectId.Value);
        command.Parameters.AddWithValue("@IsState", storedEffect.EffectId.IsState);
        command.Parameters.AddWithValue("@EffectId", storedEffect.EffectId.Value);
        command.Parameters.AddWithValue("@Status", storedEffect.WorkStatus);
        command.Parameters.AddWithValue("@Result", storedEffect.Result ?? (object) SqlBinary.Null);
        command.Parameters.AddWithValue("@Exception", JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    private static string? _setEffectResultsSql;
    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffect> storedEffects)
    {
        await using var conn = await CreateConnection();
        _setEffectResultsSql ??= $@"
             MERGE INTO {tablePrefix}_Effects
                USING (VALUES @VALUES) 
                AS source (FlowType, FlowInstance, StoredId, IsState, EffectId, Status, Result, Exception)
                ON {tablePrefix}_Effects.FlowType = source.FlowType AND {tablePrefix}_Effects.FlowInstance = source.FlowInstance AND {tablePrefix}_Effects.StoredId = source.StoredId AND {tablePrefix}_Effects.IsState = source.IsState 
                WHEN MATCHED THEN
                    UPDATE SET Status = source.Status, Result = source.Result, Exception = source.Exception 
                WHEN NOT MATCHED THEN
                    INSERT (FlowType, FlowInstance, StoredId, IsState, EffectId, Status, Result, Exception)
                    VALUES (source.FlowType, source.FlowInstance, source.StoredId, source.IsState, source.EffectId, source.Status, source.Result, source.Exception);";

        var sql = _setEffectResultsSql.Replace(
            "@VALUES",
            "(@FlowType#, @FlowInstance#, @StoredId#, @IsState#, @EffectId#, @Status#, @Result#, @Exception#)"
                .Replicate(storedEffects.Count)
                .Select((s, i) => s.Replace("#", i.ToString()))
                .StringJoin(", ")
        );
        await using var command = new SqlCommand(sql, conn);
        for (var i = 0; i < storedEffects.Count; i++)
        {
            var storedEffect = storedEffects[i];
            command.Parameters.AddWithValue($"@FlowType{i}", storedId.Type.Value);
            command.Parameters.AddWithValue($"@FlowInstance{i}", storedId.Instance.Value);
            command.Parameters.AddWithValue($"@StoredId{i}", storedEffect.StoredEffectId.Value);
            command.Parameters.AddWithValue($"@IsState{i}", storedEffect.EffectId.IsState);
            command.Parameters.AddWithValue($"@EffectId{i}", storedEffect.EffectId.Value);
            command.Parameters.AddWithValue($"@Status{i}", storedEffect.WorkStatus);
            command.Parameters.AddWithValue($"@Result{i}", storedEffect.Result ?? (object) SqlBinary.Null);
            command.Parameters.AddWithValue($"@Exception{i}", JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value);
        }

        await command.ExecuteNonQueryAsync();
    }

    private string? _getEffectResultsSql;
    public async Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _getEffectResultsSql ??= @$"
            SELECT StoredId, IsState, EffectId, Status, Result, Exception           
            FROM {tablePrefix}_Effects
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";
        
        await using var command = new SqlCommand(_getEffectResultsSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);

        var storedEffects = new List<StoredEffect>();
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows && reader.Read())
        {
            var storedEffectId = reader.GetGuid(0);
            var isState = reader.GetBoolean(1);
            var effectId = reader.GetString(2);
            
            var status = (WorkStatus) reader.GetInt32(3);
            var result = reader.IsDBNull(4) ? default : (byte[]) reader.GetValue(4);
            var exception = reader.IsDBNull(5) ? default : reader.GetString(5);

            var storedException = exception == null ? null : JsonSerializer.Deserialize<StoredException>(exception);
            var storedEffect = new StoredEffect(effectId.ToEffectId(isState), new StoredEffectId(storedEffectId), status, result, storedException);
            storedEffects.Add(storedEffect);
        }

        return storedEffects;
    }

    private string? _deleteEffectResultSql;
    public async Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId, bool isState)
    {
        await using var conn = await CreateConnection();
        _deleteEffectResultSql ??= @$"
            DELETE FROM {tablePrefix}_Effects
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND StoredId = @StoredId AND IsState = @IsState";
        
        await using var command = new SqlCommand(_deleteEffectResultSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@StoredId", effectId.Value);
        command.Parameters.AddWithValue("@IsState", isState);
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= @$"
            DELETE FROM {tablePrefix}_Effects
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";
        
        await using var command = new SqlCommand(_removeSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        
        await command.ExecuteNonQueryAsync();
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }
}