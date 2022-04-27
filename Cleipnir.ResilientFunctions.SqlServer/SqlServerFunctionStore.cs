using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerFunctionStore : IFunctionStore
{
    private readonly Func<Task<SqlConnection>> _connFunc;
    private readonly string _tablePrefix;

    public SqlServerFunctionStore(string connectionString, string tablePrefix = "")
        : this(CreateConnection(connectionString), tablePrefix) {}
    
    public SqlServerFunctionStore(Func<Task<SqlConnection>> connFunc, string tablePrefix = "")
    {
        _connFunc = connFunc;
        _tablePrefix = tablePrefix;
    }

    //todo add index for status and subsequently postponed until!
    
    private static Func<Task<SqlConnection>> CreateConnection(string connectionString)
    {
        return async () =>
        {
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return connection;
        };
    }

    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        try
        {
            await conn.ExecuteAsync(@$"
                    CREATE TABLE {_tablePrefix}RFunctions (
                        {nameof(Row.FunctionTypeId)} NVARCHAR(200) NOT NULL,
                        {nameof(Row.FunctionInstanceId)} NVARCHAR(200) NOT NULL,
                        {nameof(Row.ParamJson)} NVARCHAR(MAX) NULL,
                        {nameof(Row.ParamType)} NVARCHAR(255) NULL,
                        {nameof(Row.ScrapbookJson)} NVARCHAR(MAX) NULL,
                        {nameof(Row.ScrapbookType)} NVARCHAR(255) NULL,
                        {nameof(Row.Status)} INT NOT NULL,
                        {nameof(Row.ResultJson)} NVARCHAR(MAX) NULL,
                        {nameof(Row.ResultType)} NVARCHAR(255) NULL,
                        {nameof(Row.ErrorJson)} NVARCHAR(MAX) NULL,
                        {nameof(Row.PostponedUntil)} BIGINT NULL,
                        {nameof(Row.Epoch)} INT NOT NULL,
                        {nameof(Row.SignOfLife)} INT NOT NULL,
                        PRIMARY KEY ({nameof(Row.FunctionTypeId)}, {nameof(Row.FunctionInstanceId)})
                    );"
            );
        }
        catch (SqlException e)
        {
            if (e.Number != SqlError.TABLE_ALREADY_EXISTS)
                throw;
        }
    }

    public async Task DropIfExists()
    {
        await using var conn = await _connFunc();
        await conn.ExecuteAsync($"DROP TABLE IF EXISTS {_tablePrefix}RFunctions ");
    }

    public async Task Truncate()
    {
        await using var conn = await _connFunc();
        await conn.ExecuteAsync($"TRUNCATE TABLE {_tablePrefix}RFunctions");
    }

    public async Task<bool> CreateFunction(
        FunctionId functionId,
        StoredParameter param,
        string? scrapbookType,
        Status initialStatus,
        int initialEpoch,
        int initialSignOfLife
    )
    {
        await using var conn = await _connFunc();
        try
        {
            await conn.ExecuteAsync(@$"
                INSERT INTO {_tablePrefix}RFunctions(
                    FunctionTypeId, FunctionInstanceId, 
                    ParamJson, ParamType, 
                    ScrapbookType, 
                    Status,
                    Epoch, SignOfLife)
                VALUES(
                    @FunctionTypeId, @FunctionInstanceId, 
                    @ParamJson, @ParamType,  
                    @ScrapbookType,
                    @Status,
                    @Epoch, @SignOfLife)",
                new
                {
                    FunctionTypeId = functionId.TypeId.Value,
                    FunctionInstanceId = functionId.InstanceId.Value,
                    ParamJson = param.ParamJson,
                    ParamType = param.ParamType,
                    ScrapbookType = scrapbookType,
                    Status = initialStatus,
                    Epoch = initialEpoch,
                    SignOfLife = initialSignOfLife
                });
        }
        catch (SqlException sqlException)
        {
            if (sqlException.Number == SqlError.UNIQUENESS_VIOLATION) return false;
        }

        return true;
    }

    public async Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch)
    {
        await using var conn = await _connFunc();
        var affectedRows = await conn.ExecuteAsync(@$"
                UPDATE {_tablePrefix}RFunctions
                SET Epoch = @NewEpoch, Status = @NewStatus
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch",
            new
            {
                FunctionTypeId = functionId.TypeId.Value,
                FunctionInstanceId = functionId.InstanceId.Value,
                NewStatus = newStatus,
                NewEpoch = newEpoch,
                ExpectedEpoch = expectedEpoch
            }
        );

        return affectedRows > 0;
    }

    public async Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
    {
        await using var conn = await _connFunc();
        var affectedRows = await conn.ExecuteAsync(@$"
                UPDATE {_tablePrefix}RFunctions
                SET SignOfLife = @SignOfLife
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @Epoch",
            new
            {
                FunctionTypeId = functionId.TypeId.Value,
                FunctionInstanceId = functionId.InstanceId.Value,
                Epoch = expectedEpoch,
                SignOfLife = newSignOfLife
            }
        );

        return affectedRows > 0;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
    {
        await using var conn = await _connFunc();

        var rows = await conn.QueryAsync<StoredExecutingFunctionRow>(@$"
            SELECT FunctionInstanceId, Epoch, SignOfLife
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId AND Status = {(int) Status.Executing}",
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
        await using var conn = await _connFunc();

        var rows = await conn.QueryAsync<StoredPostponedFunctionRow>(@$"
            SELECT FunctionInstanceId, Epoch, PostponedUntil
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId AND Status = {(int) Status.Postponed} AND PostponedUntil <= @PostponedUntil",
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
        int expectedEpoch
    )
    {
        await using var conn = await _connFunc();
        var affectedRows = await conn.ExecuteAsync(@$"
                UPDATE {_tablePrefix}RFunctions
                SET
                    Status = @Status,
                    ScrapbookJson = @ScrapbookJson,
                    ResultJson = @ResultJson, ResultType = @ResultType,
                    ErrorJson = @ErrorJson,
                    PostponedUntil = @PostponedUntil
                WHERE FunctionTypeId = @FunctionTypeId
                AND FunctionInstanceId = @FunctionInstanceId
                AND Epoch = @ExpectedEpoch",
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
            });
        return affectedRows > 0;
    }

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var rows = await conn.QueryAsync<Row>(@$"
                SELECT *
                FROM {_tablePrefix}RFunctions
                WHERE FunctionTypeId = @FunctionTypeId
                AND FunctionInstanceId = @FunctionInstanceId",
            new
            {
                FunctionTypeId = functionId.TypeId.Value,
                FunctionInstanceId = functionId.InstanceId.Value
            }).ToTaskList();
            
        if (rows.Count == 0)
            return default;

        var row = rows.Single();
        return new StoredFunction(
            functionId,
            Parameter: new StoredParameter(row.ParamJson, row.ParamType),
            Scrapbook: row.ScrapbookType != null ? new StoredScrapbook(row.ScrapbookJson!, row.ScrapbookType) : null,
            Status: (Status) row.Status,
            Result: row.ResultType != null ? new StoredResult(row.ResultJson!, row.ResultType) : null,
            row.ErrorJson,
            row.PostponedUntil,
            row.Epoch,
            row.SignOfLife
        );
    }

    private record Row(
        string FunctionTypeId,
        string FunctionInstanceId,
        string ParamJson,
        string ParamType,
        string? ScrapbookJson,
        string? ScrapbookType,
        int Status,
        string? ResultJson,
        string? ResultType,
        string? ErrorJson,
        long? PostponedUntil,
        int Epoch,
        int SignOfLife
    );
}