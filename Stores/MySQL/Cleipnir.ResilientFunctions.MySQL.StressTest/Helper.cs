using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests;
using Cleipnir.ResilientFunctions.StressTests.Common;
using MySql.Data.MySqlClient;

namespace Cleipnir.ResilientFunctions.MySQL.StressTest;

public class Helper : IHelper
{
    private const string ConnectionString 
        = "server=localhost;userid=root;password=Pa55word!;database=rfunctions_tests;";

    public async Task InitializeDatabaseAndInitializeAndTruncateTable()
    {
        await DatabaseHelper.CreateDatabaseIfNotExists(ConnectionString);

        var store = new MySqlFunctionStore(ConnectionString);
        await store.Initialize();
        await store.TruncateTable();
    } 
    public async Task<int> NumberOfNonCompleted()
    {
        await using var conn = new MySqlConnection(ConnectionString);
        await conn.OpenAsync();
        var sql = @$"
            SELECT COUNT(*) 
            FROM rfunctions 
            WHERE Status = {(int) Status.Executing} OR Status = {(int) Status.Postponed};";
        await using var command = new MySqlCommand(sql, conn);
        return (int) (long) (await command.ExecuteScalarAsync() ?? 0);
    }

    public async Task<int> NumberOfSuccessfullyCompleted()
    {
        await using var conn = new MySqlConnection(ConnectionString);
        await conn.OpenAsync();
        var sql = @$"SELECT COUNT(*) FROM rfunctions WHERE Status = {(int) Status.Succeeded}";
        await using var command = new MySqlCommand(sql, conn);
        return (int) (long) (await command.ExecuteScalarAsync() ?? 0);
    }

    public async Task<IFunctionStore> CreateFunctionStore()
    {
        var store = new MySqlFunctionStore(ConnectionString);
        await store.Initialize();
        return store;
    } 
}