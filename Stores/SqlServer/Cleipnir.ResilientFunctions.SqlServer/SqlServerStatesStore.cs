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

    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        var sql = @$"    
            CREATE TABLE {_tablePrefix}States (
                Id NVARCHAR(450) PRIMARY KEY,                
                State NVARCHAR(MAX),
                Type NVARCHAR(255)
            );";

        await using var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    public async Task UpsertState(FunctionId functionId, StoredState storedState)
    {
        var (functionTypeId, functionInstanceId) = functionId;
        await using var conn = await _connFunc();
        var sql = $@"
            MERGE INTO {_tablePrefix}States
                USING (VALUES (@Id, @State, @Type)) 
                AS source (Id,State,Type)
                ON {_tablePrefix}States.Id = source.Id
                WHEN MATCHED THEN
                    UPDATE SET State = source.State, Type = source.Type
                WHEN NOT MATCHED THEN
                    INSERT (Id, State, Type)
                    VALUES (source.Id, source.State, source.Type);";
        
        await using var command = new SqlCommand(sql, conn);
        var escapedId = Escaper.Escape(functionTypeId.ToString(), functionInstanceId.ToString(), storedState.StateId.ToString());    
        command.Parameters.AddWithValue("@Id", escapedId);
        command.Parameters.AddWithValue("@State", storedState.StateJson);
        command.Parameters.AddWithValue("@Type", storedState.StateType);

        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredState>> GetStates(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT Id, State, Type
            FROM {_tablePrefix}States
            WHERE Id LIKE @IdPrefix";

        var idPrefix = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value);
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@IdPrefix", idPrefix + "%");

        var storedStates = new List<StoredState>();
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows && reader.Read())
        {
            var id = reader.GetString(0);
            var stateId = Escaper.Unescape(id)[2];
            var state =  reader.GetString(1);
            var type = reader.GetString(2);
            
            var storedState = new StoredState(stateId, state, type);
            storedStates.Add(storedState);
        }

        return storedStates;
    }

    public async Task RemoveState(FunctionId functionId, StateId stateId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            DELETE FROM {_tablePrefix}States
            WHERE Id = @Id";

        var id = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value, stateId.Value);
        await using var command = new SqlCommand(sql, conn);
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