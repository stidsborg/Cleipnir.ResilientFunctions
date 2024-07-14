using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerCorrelationsStore(string connectionString, string tablePrefix = "") : ICorrelationStore
{
    private readonly Func<Task<SqlConnection>> _connFunc = CreateConnection(connectionString);

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        _initializeSql ??= @$"    
            CREATE TABLE {tablePrefix}_Correlations (
                Type NVARCHAR(200),
                Instance NVARCHAR(200),
                Correlation NVARCHAR(200),                
                PRIMARY KEY (Type, Instance, Correlation)        
            );

            CREATE INDEX IDX_{tablePrefix}_Correlations ON {tablePrefix}_Correlations (Correlation, Type, Instance);
        ";

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
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_Correlations";
        await using var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setCorrelationSql;
    public async Task SetCorrelation(FlowId flowId, string correlationId)
    {
        var (flowType, flowInstance) = flowId;
        await using var conn = await _connFunc();
        _setCorrelationSql ??= $@"
            MERGE INTO {tablePrefix}_Correlations
                USING (VALUES (@Type, @Instance, @Correlation)) 
                AS source (Type, Instance, Correlation)
                ON {tablePrefix}_Correlations.Type = source.Type AND 
                   {tablePrefix}_Correlations.Instance = source.Instance AND
                   {tablePrefix}_Correlations.Correlation = source.Correlation
                WHEN NOT MATCHED THEN
                    INSERT (Type, Instance, Correlation)
                    VALUES (source.Type, source.Instance, source.Correlation);";
        
        await using var command = new SqlCommand(_setCorrelationSql, conn);
        command.Parameters.AddWithValue("@Type", flowType.Value);
        command.Parameters.AddWithValue("@Instance", flowInstance.Value);
        command.Parameters.AddWithValue("@Correlation", correlationId);
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _getCorrelations;
    public async Task<IReadOnlyList<FlowId>> GetCorrelations(string correlationId)
    {
        await using var conn = await _connFunc();
        _getCorrelations ??= @$"
            SELECT Type, Instance
            FROM {tablePrefix}_Correlations
            WHERE Correlation = @CorrelationId";
        
        await using var command = new SqlCommand(_getCorrelations, conn);
        command.Parameters.AddWithValue("@CorrelationId", correlationId);

        var functions = new List<FlowId>();
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows && reader.Read())
        {
            var functionType = reader.GetString(0);
            var functionInstance = reader.GetString(1);
            functions.Add(new FlowId(functionType, functionInstance));
        }

        return functions;
    }

    private string? _getCorrelationsForFunction;
    public async Task<IReadOnlyList<string>> GetCorrelations(FlowId flowId)
    {
        var (typeId, instanceId) = flowId;
        await using var conn = await _connFunc();
        _getCorrelationsForFunction ??= @$"
            SELECT Correlation
            FROM {tablePrefix}_Correlations
            WHERE Type = @FunctionType AND Instance = @FunctionInstance";
        
        await using var command = new SqlCommand(_getCorrelationsForFunction, conn);
        command.Parameters.AddWithValue("@FunctionType", typeId.Value);
        command.Parameters.AddWithValue("@FunctionInstance", instanceId.Value);

        var correlations = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows && reader.Read())
        {
            var correlation = reader.GetString(0);
            correlations.Add(correlation);
        }

        return correlations;
    }

    private string? _removeCorrelationsSql;
    public async Task RemoveCorrelations(FlowId flowId)
    {
        var (typeId, instanceId) = flowId;
        await using var conn = await _connFunc();
        _removeCorrelationsSql ??= @$"
            DELETE FROM {tablePrefix}_Correlations
            WHERE Type = @Type AND Instance = @Instance";
        
        await using var command = new SqlCommand(_removeCorrelationsSql, conn);
        command.Parameters.AddWithValue("@Type", typeId.Value);
        command.Parameters.AddWithValue("@Instance", instanceId.Value);
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _removeCorrelationSql;
    public async Task RemoveCorrelation(FlowId flowId, string correlationId)
    {
        var (typeId, instanceId) = flowId;
        await using var conn = await _connFunc();
        _removeCorrelationSql ??= @$"
            DELETE FROM {tablePrefix}_Correlations
            WHERE Type = @Type AND Instance = @Instance AND Correlation = @Correlation";
        
        await using var command = new SqlCommand(_removeCorrelationSql, conn);
        command.Parameters.AddWithValue("@Type", typeId.Value);
        command.Parameters.AddWithValue("@Instance", instanceId.Value);
        command.Parameters.AddWithValue("@Correlation", correlationId);
        
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