using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
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
        await using var conn = await _connFunc();
        var sql = @$"    
            CREATE TABLE {_tablePrefix}Activities (
                Id NVARCHAR(450) PRIMARY KEY,
                Status INT NOT NULL,
                Result NVARCHAR(MAX),
                Exception NVARCHAR(MAX)
            );";

        await using var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }
    
    public async Task SetActivityResult(FunctionId functionId, StoredActivity storedActivity)
    {
        var (functionTypeId, functionInstanceId) = functionId;
        await using var conn = await _connFunc();
        var sql = $@"
            MERGE INTO {_tablePrefix}Activities
                USING (VALUES (@Id,@Status,@Result,@Exception)) 
                AS source (Id,Status,Result,Exception)
                ON {_tablePrefix}Activities.Id = source.Id
                WHEN MATCHED THEN
                    UPDATE SET Status = source.Status, Result = source.Result, Exception = source.Exception 
                WHEN NOT MATCHED THEN
                    INSERT (Id, Status, Result, Exception)
                    VALUES (source.Id, source.Status, source.Result, source.Exception);";
        
        await using var command = new SqlCommand(sql, conn);
        var escapedId = Escaper.Escape("|", functionTypeId.ToString(), functionInstanceId.ToString(), storedActivity.ActivityId);    
        command.Parameters.AddWithValue("@Id", escapedId);
        command.Parameters.AddWithValue("@Status", storedActivity.WorkStatus);
        command.Parameters.AddWithValue("@Result", storedActivity.Result ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@Exception", JsonHelper.ToJson(storedActivity.StoredException) ?? (object) DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredActivity>> GetActivityResults(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT Id, Status, Result, Exception
            FROM {_tablePrefix}Activities
            WHERE Id LIKE @IdPrefix";

        var idPrefix = Escaper.Escape("|", functionId.TypeId.Value, functionId.InstanceId.Value);
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@IdPrefix", idPrefix + "%");

        var storedActivities = new List<StoredActivity>();
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows && reader.Read())
        {
            var id = reader.GetString(0);
            var activityId = Escaper.Unescape(id, '|', 3)[2];
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