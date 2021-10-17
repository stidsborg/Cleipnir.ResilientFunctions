using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
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
                        FunctionTypeId NVARCHAR(200),
                        FunctionInstanceId NVARCHAR(200),
                        Param NVARCHAR(MAX) NOT NULL,
                        ParamType NVARCHAR(255) NOT NULL,
                        Result NVARCHAR(MAX) NULL,
                        ResultType NVARCHAR(255) NULL,
                        LastSignOfLife BIGINT NOT NULL,
                        PRIMARY KEY (FunctionTypeId, FunctionInstanceId)
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
        
        public async Task<bool> StoreFunction(FunctionId functionId, string paramJson, string paramType, long initialSignOfLife)
        {
            await using var conn = await _connFunc();

            try
            {
                await conn.ExecuteAsync(@$"
                INSERT INTO RFunctions
                    (FunctionTypeId, FunctionInstanceId, Param, ParamType, LastSignOfLife)
                VALUES
                    (@FunctionTypeId, @FunctionInstanceId, @Param, @ParamType, @LastSignOfLife)",
                    new
                    {
                        FunctionTypeId = functionId.TypeId.Value,
                        FunctionInstanceId = functionId.InstanceId.Value,
                        Param = paramJson,
                        ParamType = paramType,
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

        public async Task<IEnumerable<StoredFunction>> GetNonCompletedFunctions(FunctionTypeId functionTypeId, long olderThan)
        {
            await using var conn = await _connFunc();

            var rows = await conn.QueryAsync<NonCompletedFunctionRow>(@$"
                SELECT FunctionInstanceId, Param, ParamType, LastSignOfLife
                FROM {_tablePrefix}RFunctions
                WHERE Result IS NULL AND FunctionTypeId = @FunctionTypeId AND LastSignOfLife < @LastSignOfLife;",
                new { FunctionTypeId = functionTypeId.Value, LastSignOfLife = olderThan }
            );

            return rows
                .Select(row => 
                    new StoredFunction(
                        new FunctionId(functionTypeId, row.FunctionInstanceId.ToFunctionInstanceId()),
                        row.Param,
                        row.ParamType,
                        row.LastSignOfLife
                    )
                ).ToList();
        }

        private record NonCompletedFunctionRow(
            string FunctionInstanceId,
            string Param,
            string ParamType,
            long LastSignOfLife
        );

        public async Task<bool> UpdateSignOfLife(FunctionId functionId, long expectedSignOfLife, long newSignOfLife)
        {
            await using var conn = await _connFunc();

            var affectedRows = await conn.ExecuteAsync(@$"
                UPDATE {_tablePrefix}RFunctions
                SET LastSignOfLife = @NewSignOfLife
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND LastSignOfLife = @ExpectedSignOfLife",
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
                SET Result = @Result, ResultType = @ResultType
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId",
                new
                {
                    FunctionTypeId = functionId.TypeId.Value,
                    FunctionInstanceId = functionId.InstanceId.Value,
                    Result = resultJson,
                    ResultType = resultType
                }
            );
        }

        public async Task<FunctionResult?> GetFunctionResult(FunctionId functionId)
        {
            await using var conn = await _connFunc();

            var rows = await conn.QueryAsync<FunctionResultRow>(@$"
                SELECT Result, ResultType
                FROM {_tablePrefix}RFunctions
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId",
                new {FunctionTypeId = functionId.TypeId.Value, FunctionInstanceId = functionId.InstanceId.Value}
            );
            var row = rows
                .Where(r => r.Result != null && r.ResultType != null)
                .Select(r => new FunctionResult(r.Result!, r.ResultType!))
                .SingleOrDefault();

            return row;
        }
        
        private record FunctionResultRow(string? Result, string? ResultType);
    }
}