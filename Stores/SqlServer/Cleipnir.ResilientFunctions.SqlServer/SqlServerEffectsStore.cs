using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
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
        await using var conn = await _connFunc();
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_Effects";
        await using var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setEffectResultSql;
    public async Task SetEffectResult(StoredId storedId, StoredEffect storedEffect)
    {
        await using var conn = await _connFunc();
        _setEffectResultSql ??= $@"
            MERGE INTO {_tablePrefix}_Effects
                USING (VALUES (@FlowType, @FlowInstance, @StoredId, @IsState, @EffectId, @Status, @Result, @Exception)) 
                AS source (FlowType, FlowInstance, StoredId, IsState, EffectId, Status, Result, Exception)
                ON {_tablePrefix}_Effects.FlowType = source.FlowType AND {_tablePrefix}_Effects.FlowInstance = source.FlowInstance AND {_tablePrefix}_Effects.StoredId = source.StoredId AND {_tablePrefix}_Effects.IsState = source.IsState 
                WHEN MATCHED THEN
                    UPDATE SET Status = source.Status, Result = source.Result, Exception = source.Exception 
                WHEN NOT MATCHED THEN
                    INSERT (FlowType, FlowInstance, StoredId, IsState, EffectId, Status, Result, Exception)
                    VALUES (source.FlowType, source.FlowInstance, source.StoredId, source.IsState, source.EffectId, source.Status, source.Result, source.Exception);";
        
        await using var command = new SqlCommand(_setEffectResultSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@StoredId", storedEffect.StoredEffectId.Value);
        command.Parameters.AddWithValue("@IsState", storedEffect.IsState);
        command.Parameters.AddWithValue("@EffectId", storedEffect.EffectId.Value);
        command.Parameters.AddWithValue("@Status", storedEffect.WorkStatus);
        command.Parameters.AddWithValue("@Result", storedEffect.Result ?? (object) SqlBinary.Null);
        command.Parameters.AddWithValue("@Exception", JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    private static string? _setEffectResultsSql;
    public async Task SetEffectResults(StoredId storedId, IEnumerable<StoredEffect> storedEffects)
    {
        /*
        var (flowType, flowInstance) = storedId;
        await using var conn = await _connFunc();
        _setEffectResultSql ??= $@"
            MERGE INTO {_tablePrefix}_Effects
                USING (VALUES @VALUES) 
                AS source (Id,IsState,Status,Result,Exception)
                ON {_tablePrefix}_Effects.Id = source.Id AND {_tablePrefix}_Effects.IsState = source.IsState 
                WHEN MATCHED THEN
                    UPDATE SET Status = source.Status, Result = source.Result, Exception = source.Exception 
                WHEN NOT MATCHED THEN
                    INSERT (Id, IsState, Status, Result, Exception)
                    VALUES (source.Id, source.IsState, source.Status, source.Result, source.Exception);";
        
        await using var command = new SqlCommand(_setEffectResultSql, conn);
        //(@Id,@IsState,@Status,@Result,@Exception)
        
        var escapedId = Escaper.Escape(flowType.Value.ToString(), flowInstance.Value.ToString(), storedEffect.EffectId.ToString());    
        command.Parameters.AddWithValue("@Id", escapedId);
        command.Parameters.AddWithValue("@IsState", storedEffect.IsState);
        command.Parameters.AddWithValue("@Status", storedEffect.WorkStatus);
        command.Parameters.AddWithValue("@Result", storedEffect.Result ?? (object) SqlBinary.Null);
        command.Parameters.AddWithValue("@Exception", JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value);

        await command.ExecuteNonQueryAsync();*/
        throw new NotImplementedException();
    }

    private string? _getEffectResultsSql;
    public async Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
    {
        await using var conn = await _connFunc();
        _getEffectResultsSql ??= @$"
            SELECT StoredId, IsState, EffectId, Status, Result, Exception           
            FROM {_tablePrefix}_Effects
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
            var storedEffect = new StoredEffect(effectId, new StoredEffectId(storedEffectId), isState, status, result, storedException);
            storedEffects.Add(storedEffect);
        }

        return storedEffects;
    }

    private string? _deleteEffectResultSql;
    public async Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId, bool isState)
    {
        await using var conn = await _connFunc();
        _deleteEffectResultSql ??= @$"
            DELETE FROM {_tablePrefix}_Effects
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
        await using var conn = await _connFunc();
        _removeSql ??= @$"
            DELETE FROM {_tablePrefix}_Effects
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";
        
        await using var command = new SqlCommand(_removeSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        
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