using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Dapper;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix; //todo use this!

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
    //todo add index for status and subsequently postponed until!

    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        await conn.ExecuteAsync(@"
            CREATE TABLE IF NOT EXISTS RFunctions (
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
            )"
        );
    }

    public async Task DropIfExists()
    {
        await using var conn = await CreateConnection();
        await conn.ExecuteAsync("DROP TABLE IF EXISTS RFunctions");
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
        var affectedRows = await conn.ExecuteAsync(@"
            INSERT INTO RFunctions
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
        var affectedRows = await conn.ExecuteAsync(@"
            UPDATE RFunctions
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
        var affectedRows = await conn.ExecuteAsync(@"
            UPDATE RFunctions
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

    public async Task<IEnumerable<StoredFunctionStatus>> GetFunctionsWithStatus(
        FunctionTypeId functionTypeId, 
        Status status, 
        long? expiresBefore = null)
    {
        await using var conn = await CreateConnection();
        if (expiresBefore == null)
        {
            var rows = await conn.QueryAsync<FunctionWithStatusRow>(@"
                SELECT function_instance_id AS FunctionInstanceId, epoch, sign_of_life AS SignOfLife, status, postponed_until AS PostponedUntil
                FROM RFunctions
                WHERE function_type_id = @FunctionTypeId AND status = @Status;",
                new {FunctionTypeId = functionTypeId.Value, Status = (int) status}
            );
            return rows.Select(row => new StoredFunctionStatus(
                row.FunctionInstanceId,
                row.Epoch,
                row.SignOfLife,
                (Status) row.Status,
                row.PostponedUntil
            )).ToList();
        }
        else
        {
            var rows = await conn.QueryAsync<FunctionWithStatusRow>(@"
                SELECT function_instance_id AS FunctionInstanceId, epoch, sign_of_life AS SignOfLife, status, postponed_until AS PostponedUntil
                FROM RFunctions
                WHERE function_type_id = @FunctionTypeId AND status = @Status AND postponed_until < @ExpiresBefore;",
                new {FunctionTypeId = functionTypeId.Value, Status = (int) status, ExpiresBefore = expiresBefore.Value}
            );
            return rows.Select(row => new StoredFunctionStatus(
                row.FunctionInstanceId,
                row.Epoch,
                row.SignOfLife,
                (Status) row.Status,
                row.PostponedUntil
            )).ToList();
        }
    }

    private record FunctionWithStatusRow(string FunctionInstanceId, int Epoch, int SignOfLife, int Status, long? PostponedUntil);

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
        var affectedRows = await conn.ExecuteAsync(@"
            UPDATE RFunctions
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
        var rows = await conn.QueryAsync<StoredFunctionRow>(@"
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
            FROM RFunctions
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