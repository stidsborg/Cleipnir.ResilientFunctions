using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgresActivityStore : IActivityStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public PostgresActivityStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        
        await using var conn = await CreateConnection();
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunction_activities (
                id VARCHAR(450) PRIMARY KEY,
                status INT NOT NULL,
                result TEXT NULL,
                exception TEXT NULL
            );";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task SetActivityResult(FunctionId functionId, StoredActivity storedActivity)
    {
        var (functionTypeId, functionInstanceId) = functionId;
        await using var conn = await CreateConnection();
        var sql = $@"
          INSERT INTO {_tablePrefix}rfunction_activities 
              (id, status, result, exception)
          VALUES
              ($1, $2, $3, $4) 
          ON CONFLICT (id) 
          DO 
            UPDATE SET status = EXCLUDED.status, result = EXCLUDED.result, exception = EXCLUDED.exception";
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(delimiter: "|", functionTypeId.Value, functionInstanceId.Value, storedActivity.ActivityId)},
                new() {Value = (int) storedActivity.WorkStatus},
                new() {Value = storedActivity.Result ?? (object) DBNull.Value},
                new() {Value = JsonHelper.ToJson(storedActivity.StoredException) ?? (object) DBNull.Value}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredActivity>> GetActivityResults(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT id, status, result, exception
            FROM {_tablePrefix}rfunction_activities
            WHERE id LIKE $1";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(delimiter: "|", functionId.TypeId.Value, functionId.InstanceId.Value) + "%"},
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredActivity>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0);
            var activityId = Escaper.Unescape(id, delimiter: '|', arraySize: 3)[2];
            var status = (WorkStatus) reader.GetInt32(1);
            var result = reader.IsDBNull(2) ? null : reader.GetString(2);
            var exception = reader.IsDBNull(3) ? null : reader.GetString(3);
            functions.Add(new StoredActivity(activityId, status, result, JsonHelper.FromJson<StoredException>(exception)));
        }

        return functions;
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}