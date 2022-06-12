using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests;
using Cleipnir.ResilientFunctions.StressTests.Common;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL.StressTest;

public class Helper : IHelper
{
    private const string ConnectionString 
        = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=rfunctions_stresstest;";

    public async Task InitializeDatabaseAndInitializeAndTruncateTable()
    {
        await DatabaseHelper.CreateDatabaseIfNotExists(ConnectionString);
        
        var store = new PostgreSqlFunctionStore(ConnectionString);
        await store.Initialize();
        await store.TruncateTable();
    }

    public async Task<int> NumberOfNonCompleted()
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync();
        var sql = @$"
            SELECT COUNT(*) 
            FROM rfunctions 
            WHERE Status = {(int) Status.Executing} OR Status = {(int) Status.Postponed};";
        await using var command = new NpgsqlCommand(sql, conn);
        return (int) (long) (await command.ExecuteScalarAsync() ?? 0);
    }
    
    public async Task<int> NumberOfSuccessfullyCompleted()
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync();
        var sql = @$"SELECT COUNT(*) FROM rfunctions WHERE Status = {(int) Status.Succeeded}";
        await using var command = new NpgsqlCommand(sql, conn);
        return (int) (long) (await command.ExecuteScalarAsync() ?? 0);
    }

    public async Task<IFunctionStore> CreateFunctionStore()
    {
        var store = new PostgreSqlFunctionStore(ConnectionString);
        await store.Initialize();
        return store;
    } 
}