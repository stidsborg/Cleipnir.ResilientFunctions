﻿using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.MySQL;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;
using DatabaseHelper = Cleipnir.ResilientFunctions.MySQL.DatabaseHelper;

namespace Cleipnir.ResilientFunctions.StressTests.Engines;

public class MySqlEngine : IEngine
{
    private const string ConnectionString 
        = "server=localhost;userid=root;password=Pa55word!;database=rfunctions_tests;";

    public async Task RecreateDatabase()
    {
        await DatabaseHelper.DropDatabaseIfExists(ConnectionString);
        await InitializeDatabaseAndInitializeAndTruncateTable();
    }

    public async Task InitializeDatabaseAndInitializeAndTruncateTable()
    {
        await DatabaseHelper.CreateDatabaseIfNotExists(ConnectionString);

        var store = new MySqlFunctionStore(ConnectionString);
        await store.Initialize();
        await store.TruncateTables();
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