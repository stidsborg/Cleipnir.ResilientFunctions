using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests;
using Dapper;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer.StressTest;

public class Helper : IHelper
{
    private const string ConnectionString = "Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!";

    public async Task InitializeDatabaseAndTruncateTable()
    {
        await DatabaseHelper.CreateDatabaseIfNotExists(ConnectionString);
        await using var conn = new SqlConnection(ConnectionString);
        conn.Open();
        try { await conn.ExecuteAsync("TRUNCATE TABLE rfunctions"); }
        catch
        {
            // ignored
        }
    }

    public async Task<int> NumberOfNonCompleted()
    {
        await using var conn = new SqlConnection(ConnectionString);
        conn.Open();
        var nonCompletes = await conn.ExecuteScalarAsync<int>(@$"
                SELECT COUNT(*) 
                FROM rfunctions 
                WHERE Status = {(int) Status.Executing} OR Status = {(int) Status.Postponed};"
        );
        return nonCompletes;
    }

    public async Task<IFunctionStore> CreateFunctionStore()
    {
        var store = new SqlServerFunctionStore(ConnectionString);
        await store.Initialize();
        return store;
    } 
}