using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerFunctionStore : IFunctionStore
{
    private readonly Func<Task<SqlConnection>> _connFunc;
    private readonly string _tablePrefix;

    private readonly SqlServerTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    
    private readonly SqlServerEventStore _eventStore;
    public IEventStore EventStore => _eventStore;
    
    public SqlServerFunctionStore(string connectionString, string tablePrefix = "")
    {
        _connFunc = CreateConnection(connectionString);
        _tablePrefix = tablePrefix;
        _eventStore = new SqlServerEventStore(connectionString, tablePrefix);
        _timeoutStore = new SqlServerTimeoutStore(connectionString, tablePrefix);
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

    public async Task Initialize()
    {
        await _eventStore.Initialize();
        await _timeoutStore.Initialize();
        await using var conn = await _connFunc();
        var sql = @$"
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{_tablePrefix}RFunctions' and xtype='U')
            BEGIN
                CREATE TABLE {_tablePrefix}RFunctions (
                    FunctionTypeId NVARCHAR(200) NOT NULL,
                    FunctionInstanceId NVARCHAR(200) NOT NULL,
                    ParamJson NVARCHAR(MAX) NOT NULL,
                    ParamType NVARCHAR(255) NOT NULL,
                    ScrapbookJson NVARCHAR(MAX) NOT NULL,
                    ScrapbookType NVARCHAR(255) NOT NULL,
                    Status INT NOT NULL,
                    ResultJson NVARCHAR(MAX) NULL,
                    ResultType NVARCHAR(255) NULL,
                    ExceptionJson NVARCHAR(MAX) NULL,
                    PostponedUntil BIGINT NULL,
                    SuspendUntilEventSourceCount INT NULL,
                    Epoch INT NOT NULL,
                    SignOfLife INT NOT NULL,
                    CrashedCheckFrequency BIGINT NOT NULL,
                    PRIMARY KEY (FunctionTypeId, FunctionInstanceId)
                );
                CREATE INDEX {_tablePrefix}RFunctions_idx_Executing
                  ON {_tablePrefix}RFunctions (FunctionTypeId, FunctionInstanceId)
                  INCLUDE (Epoch, SignOfLife)
                  WHERE Status = {(int)Status.Executing};
                CREATE INDEX {_tablePrefix}RFunctions_idx_Postponed
                  ON {_tablePrefix}RFunctions (FunctionTypeId, PostponedUntil, FunctionInstanceId)
                  INCLUDE (Epoch)
                  WHERE Status = {(int)Status.Postponed};
                CREATE INDEX {_tablePrefix}RFunctions_idx_Suspended
                  ON {_tablePrefix}RFunctions (FunctionTypeId, FunctionInstanceId)
                  INCLUDE (Epoch)
                  WHERE Status = {(int)Status.Suspended}              
            END";

        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Truncate()
    {
        await _eventStore.TruncateTable();
        await _timeoutStore.TruncateTable();
        
        await using var conn = await _connFunc();
        var sql = $"TRUNCATE TABLE {_tablePrefix}RFunctions";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> CreateFunction(FunctionId functionId, StoredParameter param, StoredScrapbook storedScrapbook, long crashedCheckFrequency)
    {
        await using var conn = await _connFunc();
        try
        {
            var sql = @$"
                INSERT INTO {_tablePrefix}RFunctions(
                    FunctionTypeId, FunctionInstanceId, 
                    ParamJson, ParamType, 
                    ScrapbookJson, ScrapbookType, 
                    Status,
                    Epoch, SignOfLife, 
                    CrashedCheckFrequency)
                VALUES(
                    @FunctionTypeId, @FunctionInstanceId, 
                    @ParamJson, @ParamType,  
                    @ScrapbookJson, @ScrapbookType,
                    {(int) Status.Executing},
                    0, 0,
                    @CrashedCheckFrequency)";
            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@ParamJson", param.ParamJson);
            command.Parameters.AddWithValue("@ParamType", param.ParamType);
            command.Parameters.AddWithValue("@ScrapbookJson", storedScrapbook.ScrapbookJson);
            command.Parameters.AddWithValue("@ScrapbookType", storedScrapbook.ScrapbookType);
            command.Parameters.AddWithValue("@CrashedCheckFrequency", crashedCheckFrequency);

            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException sqlException)
        {
            if (sqlException.Number == SqlError.UNIQUENESS_VIOLATION) return false;
        }

        return true;
    }

    public async Task<bool> IncrementEpoch(FunctionId functionId, int expectedEpoch)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Epoch = Epoch + 1
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> RestartExecution(FunctionId functionId, Tuple<StoredParameter, StoredScrapbook>? paramAndScrapbook, int expectedEpoch, long crashedCheckFrequency)
    {
        if (paramAndScrapbook == null)
        {
            await using var conn = await _connFunc();
            var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Epoch = Epoch + 1, 
                Status = {(int) Status.Executing}, 
                CrashedCheckFrequency = @CrashedCheckFrequency
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch";

            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@CrashedCheckFrequency", crashedCheckFrequency);
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows > 0;
        }
        else
        {
            await using var conn = await _connFunc();
            var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET ParamJson = @ParamJson, ParamType = @ParamType, 
                ScrapbookJson = @ScrapbookJson, ScrapbookType = @ScrapbookType, 
                Epoch = Epoch + 1, Status = {(int) Status.Executing}, 
                CrashedCheckFrequency = @CrashedCheckFrequency
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch";
            var (param, scrapbook) = paramAndScrapbook;
            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@ParamJson", param.ParamJson);
            command.Parameters.AddWithValue("@ParamType", param.ParamType);
            command.Parameters.AddWithValue("@ScrapbookJson", scrapbook.ScrapbookJson);
            command.Parameters.AddWithValue("@ScrapbookType", scrapbook.ScrapbookType);
            command.Parameters.AddWithValue("@CrashedCheckFrequency", crashedCheckFrequency);
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows > 0;
        }
    }
    
    public async Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET SignOfLife = @SignOfLife
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @Epoch";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@SignOfLife", newSignOfLife);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@Epoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT FunctionInstanceId, Epoch, SignOfLife, CrashedCheckFrequency
            FROM {_tablePrefix}RFunctions WITH (NOLOCK)
            WHERE FunctionTypeId = @FunctionTypeId AND Status = {(int) Status.Executing}";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionTypeId.Value);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<StoredExecutingFunction>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var functionInstanceId = reader.GetString(0);
                var epoch = reader.GetInt32(1);
                var signOfLife = reader.GetInt32(2);
                var crashedCheckFrequency = reader.GetInt64(3);
                rows.Add(new StoredExecutingFunction(functionInstanceId, epoch, signOfLife, crashedCheckFrequency));    
            }

            reader.NextResult();
        }

        return rows;
    }

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT FunctionInstanceId, Epoch, PostponedUntil
            FROM {_tablePrefix}RFunctions 
            WHERE FunctionTypeId = @FunctionTypeId 
              AND Status = {(int) Status.Postponed} 
              AND PostponedUntil <= @PostponedUntil";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionTypeId.Value);
        command.Parameters.AddWithValue("@PostponedUntil", expiresBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<StoredPostponedFunction>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var functionInstanceId = reader.GetString(0);
                var epoch = reader.GetInt32(1);
                var postponedUntil = reader.GetInt64(2);
                rows.Add(new StoredPostponedFunction(functionInstanceId, epoch, postponedUntil));    
            }

            reader.NextResult();
        }

        return rows;
    }

    public async Task<IEnumerable<StoredEligibleSuspendedFunction>> GetEligibleSuspendedFunctions(FunctionTypeId functionTypeId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT rf.FunctionInstanceId, rf.Epoch
            FROM {_tablePrefix}RFunctions AS rf
            INNER JOIN (
                SELECT events.FunctionInstanceId, MAX(Position) + 1 AS EventsCount
                FROM {_tablePrefix}RFunctions_Events AS events
                WHERE events.FunctionTypeId = @FunctionTypeId
                GROUP BY events.FunctionInstanceId
            ) AS events
            ON rf.FunctionInstanceId = events.FunctionInstanceId AND 
               rf.SuspendUntilEventSourceCount <= events.EventsCount
            WHERE rf.FunctionTypeId = @FunctionTypeId AND 
                  rf.status = {(int) Status.Suspended}";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionTypeId.Value);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<StoredEligibleSuspendedFunction>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var functionInstanceId = reader.GetString(0);
                var epoch = reader.GetInt32(1);
                rows.Add(new StoredEligibleSuspendedFunction(functionInstanceId, epoch));    
            }

            reader.NextResult();
        }

        return rows;
    }

    public async Task<Epoch?> IsFunctionSuspendedAndEligibleForReInvocation(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT rf.Epoch
            FROM {_tablePrefix}RFunctions AS rf
            INNER JOIN (
                SELECT MAX(Position) + 1 AS EventsCount
                FROM {_tablePrefix}RFunctions_Events AS events
                WHERE events.FunctionTypeId = @FunctionTypeId AND 
                      events.FunctionInstanceId = @FunctionInstanceId
            ) AS events
            ON 1 = 1 
            WHERE rf.FunctionTypeId = @FunctionTypeId AND
                  rf.FunctionInstanceId = @FunctionInstanceId AND
                  rf.status = {(int) Status.Suspended} AND 
                  rf.SuspendUntilEventSourceCount <= events.EventsCount";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);

        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows)
            while (reader.Read())
                return new Epoch(reader.GetInt32(0));

        return default;
    }

    public async Task<bool> SetFunctionState(
        FunctionId functionId, Status status, 
        StoredParameter storedParameter, StoredScrapbook storedScrapbook, StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil, int expectedEpoch)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET
                Status = @Status,
                ParamJson = @ParamJson, ParamType = @ParamType,            
                ScrapbookJson = @ScrapbookJson, ScrapbookType = @ScrapbookType,
                ResultJson = @ResultJson, ResultType = @ResultType,
                ExceptionJson = @ExceptionJson,
                PostponedUntil = @PostponedUntil,
                Epoch = Epoch + 1
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@Status", (int) status);
        command.Parameters.AddWithValue("@ParamJson", storedParameter.ParamJson);
        command.Parameters.AddWithValue("@ParamType", storedParameter.ParamType);
        command.Parameters.AddWithValue("@ScrapbookJson", storedScrapbook.ScrapbookJson);
        command.Parameters.AddWithValue("@ScrapbookType", storedScrapbook.ScrapbookType);
        command.Parameters.AddWithValue("@ResultJson", storedResult?.ResultJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ResultType", storedResult?.ResultType ?? (object) DBNull.Value);
        var exceptionJson = storedException == null ? null : JsonSerializer.Serialize(storedException);
        command.Parameters.AddWithValue("@ExceptionJson", exceptionJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@PostponedUntil", postponeUntil ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SaveScrapbookForExecutingFunction(FunctionId functionId, string scrapbookJson, int expectedEpoch)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET ScrapbookJson = @ScrapbookJson
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SetParameters(
        FunctionId functionId,
        StoredParameter storedParameter, StoredScrapbook storedScrapbook,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET ParamJson = @ParamJson, ParamType = @ParamType, ScrapbookJson = @ScrapbookJson, ScrapbookType = @ScrapbookType, Epoch = Epoch + 1
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ParamJson", storedParameter.ParamJson);
        command.Parameters.AddWithValue("@ParamType", storedParameter.ParamType);
        command.Parameters.AddWithValue("@ScrapbookJson", storedScrapbook.ScrapbookJson);
        command.Parameters.AddWithValue("@ScrapbookType", storedScrapbook.ScrapbookType);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, int expectedEpoch)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Succeeded}, ResultJson = @ResultJson, ResultType = @ResultType, ScrapbookJson = @ScrapbookJson
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ResultJson", result.ResultJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ResultType", result.ResultType ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Postponed}, PostponedUntil = @PostponedUntil, ScrapbookJson = @ScrapbookJson
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@PostponedUntil", postponeUntil);
        command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SuspendFunction(FunctionId functionId, int suspendUntilEventSourceCountAtLeast, string scrapbookJson, int expectedEpoch)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Suspended}, SuspendUntilEventSourceCount = @SuspendUntilEventSourceCount, ScrapbookJson = @ScrapbookJson
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@SuspendUntilEventSourceCount", suspendUntilEventSourceCountAtLeast);
        command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, int expectedEpoch)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Failed}, ExceptionJson = @ExceptionJson, ScrapbookJson = @ScrapbookJson
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ExceptionJson", JsonSerializer.Serialize(storedException));
        command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
    
    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT  ParamJson, ParamType,
                    ScrapbookJson, ScrapbookType,
                    Status,
                    ResultJson, ResultType,
                    ExceptionJson,
                    PostponedUntil,
                    SuspendUntilEventSourceCount,
                    Epoch, SignOfLife, 
                    CrashedCheckFrequency
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var paramJson = reader.GetString(0);
                var paramType = reader.GetString(1);
                var scrapbookJson = reader.GetString(2);
                var scrapbookType = reader.GetString(3);
                var status = (Status) reader.GetInt32(4);
                var resultJson = reader.IsDBNull(5) ? null : reader.GetString(5);
                var resultType = reader.IsDBNull(6) ? null : reader.GetString(6);
                var exceptionJson = reader.IsDBNull(7) ? null : reader.GetString(7);
                var storedException = exceptionJson == null
                    ? null
                    : JsonSerializer.Deserialize<StoredException>(exceptionJson);
                var postponedUntil = reader.IsDBNull(8) ? default(long?) : reader.GetInt64(8);
                var suspendedUntil = reader.IsDBNull(9) ? default(int?) : reader.GetInt32(9);
                var epoch = reader.GetInt32(10);
                var signOfLife = reader.GetInt32(11);
                var crashedCheckFrequency = reader.GetInt64(12);

                return new StoredFunction(
                    functionId,
                    new StoredParameter(paramJson, paramType),
                    new StoredScrapbook(scrapbookJson, scrapbookType),
                    status,
                    new StoredResult(resultJson, resultType),
                    storedException,
                    postponedUntil,
                    suspendedUntil,
                    epoch,
                    signOfLife,
                    crashedCheckFrequency
                );
            }
        }

        return null;
    }

    public async Task<StoredFunctionStatus?> GetFunctionStatus(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT Status, Epoch
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var status = (Status) reader.GetInt32(0);
                var epoch = reader.GetInt32(1);

                return new StoredFunctionStatus(functionId, status, epoch);
            }
        }

        return null;
    }

    public async Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null, Status? expectedStatus = null)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            DELETE FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId ";

        if (expectedEpoch != null)
            sql += "AND Epoch = @ExpectedEpoch ";
        if (expectedStatus != null)
            sql += "AND Status = @ExpectedStatus";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        if (expectedEpoch != null)
            command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch.Value);
        if (expectedStatus != null)
            command.Parameters.AddWithValue("@ExpectedStatus", (int) expectedStatus.Value);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
}