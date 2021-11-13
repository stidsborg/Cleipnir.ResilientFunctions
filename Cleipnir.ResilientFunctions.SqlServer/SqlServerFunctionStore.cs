using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer
{
    public class SqlServerFunctionStore : IFunctionStore
    {
        private readonly Func<Task<SqlConnection>> _connFunc;
        private readonly string _tablePrefix;
        
        private const int UNIQUENESS_VIOLATION = 2627;
        private const int TABLE_ALREADY_EXISTS = 2714;

        public SqlServerFunctionStore(Func<Task<SqlConnection>> connFunc, string tablePrefix = "")
        {
            _connFunc = connFunc;
            _tablePrefix = tablePrefix;
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
                        {nameof(Row.Param1Json)} NVARCHAR(MAX) NULL,
                        {nameof(Row.Param1Type)} NVARCHAR(255) NULL,
                        {nameof(Row.Param2Json)} NVARCHAR(MAX) NULL,
                        {nameof(Row.Param2Type)} NVARCHAR(255) NULL,
                        {nameof(Row.ScrapbookJson)} NVARCHAR(MAX) NULL,
                        {nameof(Row.ScrapbookType)} NVARCHAR(255) NULL,
                        {nameof(Row.ScrapbookVersionStamp)} INT NOT NULL DEFAULT 0,
                        {nameof(Row.ResultJson)} NVARCHAR(MAX) NULL,
                        {nameof(Row.ResultType)} NVARCHAR(255) NULL,
                        {nameof(Row.LastSignOfLife)} BIGINT NOT NULL DEFAULT 0,
                        PRIMARY KEY ({nameof(Row.FunctionTypeId)}, {nameof(Row.FunctionInstanceId)})
                    );"
                );
            }
            catch (SqlException e)
            {
                if (e.Number != TABLE_ALREADY_EXISTS)
                    throw;
            }
        }

        public async Task Truncate()
        {
            await using var conn = await _connFunc();
            await conn.ExecuteAsync($"TRUNCATE TABLE {_tablePrefix}RFunctions");
        }

        public async Task<bool> StoreFunction(
            FunctionId functionId, 
            Parameter param1, 
            Parameter? param2, 
            string? scrapbookType,
            long initialSignOfLife
        )
        {
            await using var conn = await _connFunc();

            try
            {
                await conn.ExecuteAsync(@$"
                INSERT INTO {_tablePrefix}RFunctions
                    (
                        {nameof(Row.FunctionTypeId)}, {nameof(Row.FunctionInstanceId)}, 
                        {nameof(Row.Param1Json)}, {nameof(Row.Param1Type)}, 
                        {nameof(Row.Param2Json)}, {nameof(Row.Param2Type)},
                        {nameof(Row.ScrapbookType)}, 
                        {nameof(Row.LastSignOfLife)}
                    )
                VALUES
                    (
                        @FunctionTypeId, @FunctionInstanceId, 
                        @Param1Json, @Param1Type, 
                        @Param2Json, @Param2Type,
                        @ScrapbookType, 
                        @LastSignOfLife
                    )",
                    new
                    {
                        FunctionTypeId = functionId.TypeId.Value,
                        FunctionInstanceId = functionId.InstanceId.Value,
                        Param1Json = param1.ParamJson,
                        Param1Type = param1.ParamType,
                        Param2Json = param2?.ParamJson,
                        Param2Type = param2?.ParamType,
                        ScrapbookType = scrapbookType,
                        LastSignOfLife = initialSignOfLife
                    }
                );
            }
            catch (SqlException e)
            {
                if (e.Number == UNIQUENESS_VIOLATION)
                    return false;
            }

            return true;
        }

        public async Task<bool> UpdateScrapbook(FunctionId functionId, string scrapbookJson, int expectedVersionStamp, int newVersionStamp)
        {
            await using var conn = await _connFunc();

            var affectedRows = await conn.ExecuteAsync(@$"
                UPDATE {_tablePrefix}RFunctions
                SET {nameof(Row.ScrapbookJson)} = @ScrapbookJson, {nameof(Row.ScrapbookVersionStamp)} = @NewVersionStamp
                WHERE {nameof(Row.FunctionTypeId)} = @FunctionTypeId AND 
                      {nameof(Row.FunctionInstanceId)} = @FunctionInstanceId AND
                      {nameof(Row.ScrapbookVersionStamp)} = @ExpectedVersionStamp",
                new
                {
                    FunctionTypeId = functionId.TypeId.Value,
                    FunctionInstanceId = functionId.InstanceId.Value,
                    ScrapbookJson = scrapbookJson,
                    ExpectedVersionStamp = expectedVersionStamp,
                    NewVersionStamp = newVersionStamp
                }
            );
            return affectedRows == 1;
        }

        public async Task<IEnumerable<NonCompletedFunction>> GetNonCompletedFunctions(FunctionTypeId functionTypeId)
        {
            await using var conn = await _connFunc();
            
            var rows = await QueryAsync(@$"
                SELECT {nameof(Row.FunctionInstanceId)}, {nameof(Row.LastSignOfLife)}
                FROM {_tablePrefix}RFunctions
                WHERE {nameof(Row.ResultType)} IS NULL AND {nameof(Row.FunctionTypeId)} = @FunctionTypeId;",
                new { FunctionTypeId = functionTypeId.Value },
                new { FunctionInstanceId = default(string), LastSignOfLife = default(long) }
            );

            return rows
                .Select(r =>
                    new NonCompletedFunction(r.FunctionInstanceId!.ToFunctionInstanceId(), r.LastSignOfLife))
                .ToList();
        }

        public async Task<bool> UpdateSignOfLife(FunctionId functionId, long expectedSignOfLife, long newSignOfLife)
        {
            await using var conn = await _connFunc();

            var affectedRows = await conn.ExecuteAsync(@$"
                UPDATE {_tablePrefix}RFunctions
                SET {nameof(Row.LastSignOfLife)} = @NewSignOfLife
                WHERE {nameof(Row.FunctionTypeId)} = @FunctionTypeId 
                    AND {nameof(Row.FunctionInstanceId)} = @FunctionInstanceId 
                    AND {nameof(Row.LastSignOfLife)} = @ExpectedSignOfLife",
                new
                {
                    FunctionTypeId = functionId.TypeId.Value,
                    FunctionInstanceId = functionId.InstanceId.Value,
                    ExpectedSignOfLife = expectedSignOfLife,
                    NewSignOfLife = newSignOfLife
                }
            );

            return affectedRows == 1;
        }

        public async Task StoreFunctionResult(FunctionId functionId, string resultJson, string resultType)
        {
            await using var conn = await _connFunc();

            await conn.ExecuteAsync(@$"
                UPDATE {_tablePrefix}RFunctions
                SET {nameof(Row.ResultJson)} = @ResultJson, {nameof(Row.ResultType)} = @ResultType
                WHERE {nameof(Row.FunctionTypeId)} = @FunctionTypeId AND {nameof(Row.FunctionInstanceId)} = @FunctionInstanceId",
                new
                {
                    FunctionTypeId = functionId.TypeId.Value,
                    FunctionInstanceId = functionId.InstanceId.Value,
                    ResultJson = resultJson,
                    ResultType = resultType
                }
            );
        }

        public async Task<Result?> GetFunctionResult(FunctionId functionId)
        {
            await using var conn = await _connFunc();

            var rows = await QueryAsync(@$"
                SELECT {nameof(Row.ResultJson)}, {nameof(Row.ResultType)}
                FROM {_tablePrefix}RFunctions
                WHERE {nameof(Row.FunctionTypeId)} = @FunctionTypeId 
                    AND {nameof(Row.FunctionInstanceId)} = @FunctionInstanceId",
                new {FunctionTypeId = functionId.TypeId.Value, FunctionInstanceId = functionId.InstanceId.Value},
                new { ResultJson = default(string), ResultType = default(string) }
            ).ToTaskList();

            if (rows.Count == 0)
                return null;
            
            var row = rows
                .Where(r => r.ResultType != null)
                .Select(r => new Result(r.ResultJson!, r.ResultType!))
                .SingleOrDefault();

            return row;
        }

        public async Task<StoredFunction?> GetFunction(FunctionId functionId)
        {
            await using var conn = await _connFunc();
            var rows = await conn.QueryAsync<Row>(@$"
                SELECT *
                FROM {_tablePrefix}RFunctions
                WHERE {nameof(Row.FunctionTypeId)} = @FunctionTypeId 
                    AND {nameof(Row.FunctionInstanceId)} = @FunctionInstanceId",
                new {FunctionTypeId = functionId.TypeId.Value, FunctionInstanceId = functionId.InstanceId.Value}
            ).ToTaskList();

            if (rows.Count == 0)
                return null;

            var row = rows.Single();
            return new StoredFunction(
                functionId,
                new Parameter(row.Param1Json, row.Param1Type),
                row.Param2Type == null 
                    ? null 
                    : new Parameter(row.Param2Json!, row.Param2Type),
                row.ScrapbookType == null
                    ? null
                    : new Scrapbook(row.ScrapbookJson, row.ScrapbookType, row.ScrapbookVersionStamp),
                row.LastSignOfLife,
                row.ResultType == null ? null : new Result(row.ResultJson!, row.ResultType)
            );
        }

        private async Task<IEnumerable<TRow>> QueryAsync<TParam, TRow>(string sql, TParam param, TRow row)
        {
            _ = row;
            await using var conn = await _connFunc();

            var rows = await conn.QueryAsync<TRow>(sql, param);
            return rows;
        }

        private record Row(
            string FunctionTypeId,
            string FunctionInstanceId,
            string Param1Json,
            string Param1Type,
            string? Param2Json,
            string? Param2Type,
            string? ScrapbookJson,
            string? ScrapbookType,
            int ScrapbookVersionStamp,
            string? ResultJson,
            string? ResultType,
            long LastSignOfLife
        );
    }
}