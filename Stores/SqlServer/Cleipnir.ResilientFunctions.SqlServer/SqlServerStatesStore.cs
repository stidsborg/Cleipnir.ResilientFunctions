using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerStatesStore : IStatesStore
{
    private readonly string _tablePrefix;
    private readonly Func<Task<SqlConnection>> _connFunc;

    public SqlServerStatesStore(string connectionString, string tablePrefix = "")
    {
        _tablePrefix = tablePrefix;
        _connFunc = CreateConnection(connectionString);
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        _initializeSql ??= @$"    
            CREATE TABLE {_tablePrefix}_States (
                Id NVARCHAR(450) PRIMARY KEY,                
                State NVARCHAR(MAX)
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
        _truncateSql ??= $"TRUNCATE TABLE {_tablePrefix}_States";
        await using var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _upsertStateSql;
    public async Task UpsertState(FunctionId functionId, StoredState storedState)
    {
        var (functionTypeId, functionInstanceId) = functionId;
        await using var conn = await _connFunc();
        _upsertStateSql ??= $@"
            MERGE INTO {_tablePrefix}_States
                USING (VALUES (@Id, @State)) 
                AS source (Id,State)
                ON {_tablePrefix}_States.Id = source.Id
                WHEN MATCHED THEN
                    UPDATE SET State = source.State
                WHEN NOT MATCHED THEN
                    INSERT (Id, State)
                    VALUES (source.Id, source.State);";
        
        await using var command = new SqlCommand(_upsertStateSql, conn);
        var escapedId = Escaper.Escape(functionTypeId.ToString(), functionInstanceId.ToString(), storedState.StateId.ToString());    
        command.Parameters.AddWithValue("@Id", escapedId);
        command.Parameters.AddWithValue("@State", storedState.StateJson);

        await command.ExecuteNonQueryAsync();
    }

    private string? _getStates;
    public async Task<IEnumerable<StoredState>> GetStates(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        _getStates ??= @$"
            SELECT Id, State
            FROM {_tablePrefix}_States
            WHERE Id LIKE @IdPrefix";

        var idPrefix = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value);
        await using var command = new SqlCommand(_getStates, conn);
        command.Parameters.AddWithValue("@IdPrefix", idPrefix + "%");

        var storedStates = new List<StoredState>();
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows && reader.Read())
        {
            var id = reader.GetString(0);
            var stateId = Escaper.Unescape(id)[2];
            var state =  reader.GetString(1);
            
            var storedState = new StoredState(stateId, state);
            storedStates.Add(storedState);
        }

        return storedStates;
    }

    private string? _removeStateSql;
    public async Task RemoveState(FunctionId functionId, StateId stateId)
    {
        await using var conn = await _connFunc();
        _removeStateSql ??= @$"
            DELETE FROM {_tablePrefix}_States
            WHERE Id = @Id";

        var id = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value, stateId.Value);
        await using var command = new SqlCommand(_removeStateSql, conn);
        command.Parameters.AddWithValue("@Id", id);
        
        await command.ExecuteNonQueryAsync();
    }
    
    private string? _removeSql;
    public async Task Remove(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        _removeSql ??= $"DELETE FROM {_tablePrefix}_States WHERE Id LIKE @Id";

        var idPrefix = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value) + $"{Escaper.Separator}%";
        await using var command = new SqlCommand(_removeSql, conn);
        command.Parameters.AddWithValue("@Id", idPrefix);
        
        await command.ExecuteNonQueryAsync();    }

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