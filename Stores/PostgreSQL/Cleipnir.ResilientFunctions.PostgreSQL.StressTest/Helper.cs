using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests;
using Dapper;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL.StressTest;

public class Helper : IHelper
{
    private const string ConnectionString 
        = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=rfunctions;";

    public async Task InitializeDatabaseAndTruncateTable()
    {
        await DatabaseHelper.CreateDatabaseIfNotExists(ConnectionString);
        await using var conn = new NpgsqlConnection(ConnectionString);
        conn.Open();
        await conn.ExecuteAsync(@"TRUNCATE TABLE rfunctions");
    } 
    public async Task<int> NumberOfNonCompleted()
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
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
        var store = new PostgreSqlFunctionStore(ConnectionString);
        await store.Initialize();
        return store;
    } 
}