using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Dapper;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public PostgreSqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    } 

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }

    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        await conn.ExecuteAsync($@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}RFunctions (
                function_type_id VARCHAR(200) NOT NULL,
                function_instance_id VARCHAR(200) NOT NULL,
                param_json TEXT NULL,
                param_type VARCHAR(255) NULL,
                scrapbook_json TEXT NULL,
                scrapbook_type VARCHAR(255) NULL,
                status INT NOT NULL,
                result_json TEXT NULL,
                result_type VARCHAR(255) NULL,
                error_json TEXT NULL,
                postponed_until BIGINT NULL,
                epoch INT NOT NULL,
                sign_of_life INT NOT NULL,
                PRIMARY KEY (function_type_id, function_instance_id)
            );
            CREATE INDEX IF NOT EXISTS idx_{_tablePrefix}rfunctions_executing
            ON {_tablePrefix}rfunctions(function_type_id, function_instance_id)
            INCLUDE (epoch, sign_of_life)
            WHERE status = {(int) Status.Executing};

            CREATE INDEX IF NOT EXISTS idx_{_tablePrefix}rfunctions_postponed
            ON {_tablePrefix}rfunctions(function_type_id, postponed_until, function_instance_id)
            INCLUDE (epoch)
            WHERE status = {(int) Status.Postponed};"
        );
    }

    public async Task DropIfExists()
    {
        await using var conn = await CreateConnection();
        await conn.ExecuteAsync($"DROP TABLE IF EXISTS {_tablePrefix}RFunctions");
    }

    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        await conn.ExecuteAsync($"TRUNCATE TABLE {_tablePrefix}RFunctions");
    }
    
    public async Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        string? scrapbookType, 
        Status initialStatus,
        int initialEpoch, 
        int initialSignOfLife)
    {
        await using var conn = await CreateConnection();
        var affectedRows = await conn.ExecuteAsync(@$"
            INSERT INTO {_tablePrefix}RFunctions
                (function_type_id, function_instance_id, param_json, param_type, scrapbook_type, status, epoch, sign_of_life)
            VALUES
                (@FunctionTypeId, @FunctionInstanceId, @ParamJson, @ParamType, @ScrapbookType, @Status, @Epoch, @SignOfLife)
            ON CONFLICT DO NOTHING;",
            new 
            {
                FunctionTypeId = functionId.TypeId.Value,
                FunctionInstanceId = functionId.InstanceId.Value,
                ParamJson = param.ParamJson,
                ParamType = param.ParamType,
                ScrapbookType = scrapbookType,
                Status = (int) initialStatus,
                Epoch = initialEpoch,
                SignOfLife = initialSignOfLife
            });

        return affectedRows == 1;
    }

    public async Task<bool> TryToBecomeLeader(
        FunctionId functionId, 
        Status newStatus, 
        int expectedEpoch, 
        int newEpoch)
    {
        await using var conn = await CreateConnection();
        var affectedRows = await conn.ExecuteAsync(@$"
            UPDATE {_tablePrefix}RFunctions
            SET epoch = @NewEpoch, status = @NewStatus
            WHERE function_type_id = @FunctionTypeId AND function_instance_id = @FunctionInstanceId AND epoch = @ExpectedEpoch",
            new
            {
                FunctionTypeId = functionId.TypeId.Value,
                FunctionInstanceId = functionId.InstanceId.Value,
                ExpectedEpoch = expectedEpoch,
                NewEpoch = newEpoch,
                NewStatus = (int) newStatus
            }
        );

        return affectedRows == 1;
    }

    public async Task<bool> UpdateSignOfLife(
        FunctionId functionId, 
        int expectedEpoch, 
        int newSignOfLife)
    {
        await using var conn = await CreateConnection();
        var affectedRows = await conn.ExecuteAsync($@"
            UPDATE {_tablePrefix}RFunctions
            SET sign_of_life = @NewSignOfLife
            WHERE function_type_id = @FunctionTypeId AND function_instance_id = @FunctionInstanceId AND epoch = @ExpectedEpoch",
            new
            {
                FunctionTypeId = functionId.TypeId.Value,
                FunctionInstanceId = functionId.InstanceId.Value,
                ExpectedEpoch = expectedEpoch,
                NewSignOfLife = newSignOfLife
            }
        );

        return affectedRows == 1;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
    {
        await using var conn = await CreateConnection();

        var rows = await conn.QueryAsync<StoredExecutingFunctionRow>(@$"
            SELECT function_instance_id AS FunctionInstanceId, epoch, sign_of_life AS SignOfLife 
            FROM {_tablePrefix}RFunctions
            WHERE function_type_id = @FunctionTypeId AND status = {(int) Status.Executing}",
            new { FunctionTypeId = functionTypeId.Value }
        );
        return rows
            .Select(r =>
                new StoredExecutingFunction(r.FunctionInstanceId.ToFunctionInstanceId(), r.Epoch, r.SignOfLife)
            ).ToList().AsEnumerable();
    }
    private record StoredExecutingFunctionRow(string FunctionInstanceId, int Epoch, int SignOfLife);

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
    {
        await using var conn = await CreateConnection();

        var rows = await conn.QueryAsync<StoredPostponedFunctionRow>(@$"
            SELECT function_instance_id AS FunctionInstanceId, epoch, postponed_until AS PostponedUntil
            FROM {_tablePrefix}RFunctions
            WHERE function_type_id = @FunctionTypeId AND status = {(int) Status.Postponed} AND postponed_until <= @PostponedUntil",
            new { FunctionTypeId = functionTypeId.Value, PostponedUntil = expiresBefore }
        );

        return rows
            .Select(r =>
                new StoredPostponedFunction(r.FunctionInstanceId.ToFunctionInstanceId(), r.Epoch, r.PostponedUntil)
            ).ToList().AsEnumerable();
    }
    private record StoredPostponedFunctionRow(string FunctionInstanceId, int Epoch, long PostponedUntil);

    public async Task<bool> SetFunctionState(
        FunctionId functionId, 
        Status status, 
        string? scrapbookJson, 
        StoredResult? result,
        string? errorJson, 
        long? postponedUntil, 
        int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        var affectedRows = await conn.ExecuteAsync($@"
            UPDATE {_tablePrefix}RFunctions
            SET status = @Status, scrapbook_json = @ScrapbookJson, 
                result_json = @ResultJson, result_type = @ResultType, 
                error_json = @ErrorJson, postponed_until = @PostponedUntil
            WHERE 
                function_type_id = @FunctionTypeId AND 
                function_instance_id = @FunctionInstanceId AND 
                epoch = @ExpectedEpoch",
            new
            {
                FunctionTypeId = functionId.TypeId.Value,
                FunctionInstanceId = functionId.InstanceId.Value,
                ExpectedEpoch = expectedEpoch,
                Status = (int) status,
                ScrapbookJson = scrapbookJson,
                ResultJson = result?.ResultJson,
                ResultType = result?.ResultType,
                ErrorJson = errorJson,
                PostponedUntil = postponedUntil
            }
        );

        return affectedRows == 1;
    }

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var rows = await conn.QueryAsync<StoredFunctionRow>($@"
            SELECT               
                param_json AS ParamJson, 
                param_type AS ParamType,
                scrapbook_json AS ScrapbookJson, 
                scrapbook_type AS ScrapbookType,
                status,
                result_json AS ResultJson, 
                result_type AS ResultType,
                error_json AS ErrorJson,
                postponed_until AS PostponedUntil,
                epoch, 
                sign_of_life AS SignOfLife
            FROM {_tablePrefix}RFunctions
            WHERE function_type_id = @FunctionTypeId AND function_instance_id = @FunctionInstanceId;",
            new
            {
                FunctionTypeId = functionId.TypeId.Value,
                FunctionInstanceId = functionId.InstanceId.Value
            }
        ).ToTaskList();

        if (rows.Count == 0) return null;
        
        var row = rows.Single();
        return new StoredFunction(
            functionId,
            new StoredParameter(row.ParamJson, row.ParamType),
            Scrapbook: row.ScrapbookType != null ? new StoredScrapbook(row.ScrapbookJson!, row.ScrapbookType) : null,
            Status: (Status) row.Status,
            Result: row.ResultType != null ? new StoredResult(row.ResultJson!, row.ResultType) : null,
            row.ErrorJson,
            row.PostponedUntil,
            row.Epoch,
            row.SignOfLife
        );
    }

    private record StoredFunctionRow(
        string ParamJson, string ParamType,
        string? ScrapbookJson, string? ScrapbookType,
        int Status,
        string? ResultJson, string? ResultType,
        string? ErrorJson,
        long? PostponedUntil,
        int Epoch, int SignOfLife
    );
}