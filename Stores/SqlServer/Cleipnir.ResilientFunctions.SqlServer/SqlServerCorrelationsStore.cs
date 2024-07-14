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
                Function_Type NVARCHAR(200),
                Function_Instance NVARCHAR(200),
                Correlation NVARCHAR(200),                
                PRIMARY KEY (Function_Type, Function_Instance, Correlation)        
            );

            CREATE INDEX IDX_{tablePrefix}_Correlations ON {tablePrefix}_Correlations (Correlation, Function_Type, Function_Instance);
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
        var (functionTypeId, functionInstanceId) = flowId;
        await using var conn = await _connFunc();
        _setCorrelationSql ??= $@"
            MERGE INTO {tablePrefix}_Correlations
                USING (VALUES (@Function_Type, @Function_Instance, @Correlation)) 
                AS source (Function_Type, Function_Instance, Correlation)
                ON {tablePrefix}_Correlations.Function_Type = source.Function_Type AND 
                   {tablePrefix}_Correlations.Function_Instance = source.Function_Instance AND
                   {tablePrefix}_Correlations.Correlation = source.Correlation
                WHEN NOT MATCHED THEN
                    INSERT (Function_Type, Function_Instance, Correlation)
                    VALUES (source.Function_Type, source.Function_Instance, source.Correlation);";
        
        await using var command = new SqlCommand(_setCorrelationSql, conn);
        command.Parameters.AddWithValue("@Function_Type", functionTypeId.Value);
        command.Parameters.AddWithValue("@Function_Instance", functionInstanceId.Value);
        command.Parameters.AddWithValue("@Correlation", correlationId);
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _getCorrelations;
    public async Task<IReadOnlyList<FlowId>> GetCorrelations(string correlationId)
    {
        await using var conn = await _connFunc();
        _getCorrelations ??= @$"
            SELECT Function_Type, Function_Instance
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
            WHERE Function_Type = @FunctionType AND Function_Instance = @FunctionInstance";
        
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
            WHERE Function_Type = @Function_Type AND Function_Instance = @Function_Instance";
        
        await using var command = new SqlCommand(_removeCorrelationsSql, conn);
        command.Parameters.AddWithValue("@Function_Type", typeId.Value);
        command.Parameters.AddWithValue("@Function_Instance", instanceId.Value);
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _removeCorrelationSql;
    public async Task RemoveCorrelation(FlowId flowId, string correlationId)
    {
        var (typeId, instanceId) = flowId;
        await using var conn = await _connFunc();
        _removeCorrelationSql ??= @$"
            DELETE FROM {tablePrefix}_Correlations
            WHERE Function_Type = @Function_Type AND Function_Instance = @Function_Instance AND Correlation = @Correlation";
        
        await using var command = new SqlCommand(_removeCorrelationSql, conn);
        command.Parameters.AddWithValue("@Function_Type", typeId.Value);
        command.Parameters.AddWithValue("@Function_Instance", instanceId.Value);
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