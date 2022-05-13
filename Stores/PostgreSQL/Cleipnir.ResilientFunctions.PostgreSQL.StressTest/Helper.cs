using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests;
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
        await conn.OpenAsync();
        await using var command = new NpgsqlCommand(@"TRUNCATE TABLE rfunctions", conn);
        await command.ExecuteNonQueryAsync();
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

    public async Task<IFunctionStore> CreateFunctionStore()
    {
        var store = new PostgreSqlFunctionStore(ConnectionString);
        await store.Initialize();
        return store;
    } 
}