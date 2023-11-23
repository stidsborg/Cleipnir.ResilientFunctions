using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerActivityStore : IActivityStore
{
    private readonly string _tablePrefix;
    private readonly Func<Task<SqlConnection>> _connFunc;

    public SqlServerActivityStore(string connectionString, string tablePrefix = "")
    {
        _tablePrefix = tablePrefix;
        _connFunc = CreateConnection(connectionString);
    }

    public async Task Initialize()
    {
        SHA256.Create()
        await using var conn = await _connFunc();
        var sql = @$"    
            CREATE TABLE {_tablePrefix}Activities (
                FunctionIdHash NVARCHAR(200) NOT NULL,
                ActivityId NVARCHAR(200) NOT NULL,
                Status INT NOT NULL,
                Result NVARCHAR(MAX),
                Exception NVARCHAR(MAX),
                PRIMARY KEY (FunctionTypeId, FunctionInstanceId)
            );

            CREATE UNIQUE INDEX Activities_idx
            ON {_tablePrefix}Activities (FunctionTypeId, FunctionInstanceId, ActivityId);";

        await using var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }
    
    public async Task SetActivityResult(FunctionId functionId, StoredActivity storedActivity)
    {
        await using var conn = await _connFunc();
        var sql = $@"
            MERGE INTO {_tablePrefix}Activities
                USING (VALUES (@FunctionTypeId,@FunctionInstanceId,@ActivityId,@Status,@Result,@Exception)) 
                AS source (FunctionTypeId,FunctionInstanceId,ActivityId,Status,Result,Exception)
                ON {_tablePrefix}Activities.FunctionTypeId = source.FunctionTypeId AND 
                   {_tablePrefix}Activities.FunctionInstanceId = source.FunctionInstanceId AND
                   {_tablePrefix}Activities.ActivityId = source.ActivityId
                WHEN MATCHED THEN
                    UPDATE SET Status = source.Status, Result = source.Result, Exception = source.Exception 
                WHEN NOT MATCHED THEN
                    INSERT (FunctionTypeId, FunctionInstanceId, ActivityId, Status, Result, Exception)
                    VALUES (source.FunctionTypeId, source.FunctionInstanceId, source.ActivityId, source.Status, source.Result, source.Exception);";
        
        try
        {
            await using var command = new SqlCommand(sql, conn);
            
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@ActivityId", storedActivity.ActivityId);
            command.Parameters.AddWithValue("@Status", storedActivity.WorkStatus);
            command.Parameters.AddWithValue("@Result", storedActivity.Result ?? (object) DBNull.Value);
            command.Parameters.AddWithValue("@Exception", storedActivity.StoredException.ToJson() ?? (object) DBNull.Value);

            await command.ExecuteNonQueryAsync();
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    public async Task<IEnumerable<StoredActivity>> GetActivityResults(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT ActivityId, Status, Result, Exception
            FROM {_tablePrefix}Activities
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);

        var storedActivities = new List<StoredActivity>();
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows && reader.Read())
        {
            var activityId = reader.GetString(0);
            var status = (WorkStatus) reader.GetInt32(1);
            var result = reader.IsDBNull(2) ? default : reader.GetString(2);
            var exception = reader.IsDBNull(3) ? default : reader.GetString(3);

            var storedException = exception == null ? null : JsonSerializer.Deserialize<StoredException>(exception);
            var storedActivity = new StoredActivity(activityId, status, result, storedException);
            storedActivities.Add(storedActivity);
        }

        return storedActivities;
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