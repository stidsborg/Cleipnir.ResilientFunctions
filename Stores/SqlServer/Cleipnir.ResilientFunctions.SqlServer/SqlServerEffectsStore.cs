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

public class SqlServerEffectsStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "") : IEffectsStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"    
            CREATE TABLE {tablePrefix}_Effects (
                Id UNIQUEIDENTIFIER,
                StoredId UNIQUEIDENTIFIER,              
                EffectId VARCHAR(MAX) NOT NULL,                
                Status INT NOT NULL,
                Result VARBINARY(MAX),
                Exception NVARCHAR(MAX),
                
                PRIMARY KEY (Id, StoredId)
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
    public async Task SetEffectResult(StoredId storedId, StoredEffect storedEffect, IStorageSession? session)
    {
        await using var conn = await CreateConnection();
        _setEffectResultSql ??= $@"
            MERGE INTO {tablePrefix}_Effects
                USING (VALUES (@Id, @StoredId, @EffectId, @Status, @Result, @Exception)) 
                AS source (Id, StoredId, EffectId, Status, Result, Exception)
                ON {tablePrefix}_Effects.Id = source.Id AND {tablePrefix}_Effects.StoredId = source.StoredId
                WHEN MATCHED THEN
                    UPDATE SET Status = source.Status, Result = source.Result, Exception = source.Exception 
                WHEN NOT MATCHED THEN
                    INSERT (Id, StoredId, EffectId, Status, Result, Exception)
                    VALUES (source.Id, source.StoredId, source.EffectId, source.Status, source.Result, source.Exception);";
        
        await using var command = new SqlCommand(_setEffectResultSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        command.Parameters.AddWithValue("@StoredId", storedEffect.StoredEffectId.Value);
        command.Parameters.AddWithValue("@EffectId", storedEffect.EffectId.Serialize());
        command.Parameters.AddWithValue("@Status", storedEffect.WorkStatus);
        command.Parameters.AddWithValue("@Result", storedEffect.Result ?? (object) SqlBinary.Null);
        command.Parameters.AddWithValue("@Exception", JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes)
    {
        if (changes.Count == 0)
            return;
        
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator
            .UpdateEffects(changes, paramPrefix: "")
            .ToSqlCommand(conn);
        
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetEffects(storedId).ToSqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var effects = await sqlGenerator.ReadEffects(reader);
        return effects;
    }

    public async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetEffects(storedIds).ToSqlCommand(conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        var effects = await sqlGenerator.ReadEffectsForMultipleStoredIds(reader);
        return effects;
    }

    private string? _deleteEffectResultSql;
    public async Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId)
    {
        await using var conn = await CreateConnection();
        _deleteEffectResultSql ??= @$"
            DELETE FROM {tablePrefix}_Effects
            WHERE Id = @Id AND StoredId = @StoredId";
        
        await using var command = new SqlCommand(_deleteEffectResultSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        command.Parameters.AddWithValue("@StoredId", effectId.Value);
        
        await command.ExecuteNonQueryAsync();
    }

    public async Task DeleteEffectResults(StoredId storedId, IReadOnlyList<StoredEffectId> effectIds)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            DELETE FROM {tablePrefix}_Effects 
            WHERE Id = '{storedId.AsGuid}' AND 
                  StoredId IN ({effectIds.Select(id => $"'{id.Value}'").StringJoin(", ")}) ";
        
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= @$"
            DELETE FROM {tablePrefix}_Effects
            WHERE Id = @Id";
        
        await using var command = new SqlCommand(_removeSql, conn);
        command.Parameters.AddWithValue("@Id", storedId.AsGuid);
        
        await command.ExecuteNonQueryAsync();
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }
}