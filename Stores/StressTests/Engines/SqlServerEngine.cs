using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.SqlServer;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;
using DatabaseHelper = Cleipnir.ResilientFunctions.SqlServer.DatabaseHelper;

namespace Cleipnir.ResilientFunctions.StressTests.Engines;

public class SqlServerEngine : IEngine
{
    private const string ConnectionString = "Server=localhost;Database=rfunctions_stresstest;User Id=sa;Password=Pa55word!;Encrypt=True;TrustServerCertificate=True;";

    public async Task InitializeDatabaseAndInitializeAndTruncateTable()
    {
        await DatabaseHelper.CreateDatabaseIfNotExists(ConnectionString);
        
        var store = new SqlServerFunctionStore(ConnectionString);
        await store.Initialize();
        await store.Truncate();
    }

    public async Task<int> NumberOfNonCompleted()
    {
        await using var conn = new SqlConnection(ConnectionString);
        conn.Open();
        var sql = @$"
            SELECT COUNT(*) 
            FROM rfunctions 
            WHERE Status = {(int) Status.Executing} OR Status = {(int) Status.Postponed}";
        await using var command = new SqlCommand(sql, conn);
        var count = (int) (await command.ExecuteScalarAsync() ?? 0);
        return count;
    }
    
    public async Task<int> NumberOfSuccessfullyCompleted()
    {
        await using var conn = new SqlConnection(ConnectionString);
        conn.Open();
        var sql = @$"SELECT COUNT(*) FROM rfunctions WHERE Status = {(int) Status.Succeeded}";
        await using var command = new SqlCommand(sql, conn);
        var count = (int) (await command.ExecuteScalarAsync() ?? 0);
        return count;
    }

    public async Task<IFunctionStore> CreateFunctionStore()
    {
        var store = new SqlServerFunctionStore(ConnectionString);
        await store.Initialize();
        return store;
    } 
}