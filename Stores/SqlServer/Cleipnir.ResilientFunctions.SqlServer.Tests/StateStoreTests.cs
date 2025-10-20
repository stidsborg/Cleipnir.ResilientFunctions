using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class StateStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.StateStoreTests
{
    private static async Task<SqlServerStateStore> CreateAndInitializeStateStore(
        [CallerFilePath] string sourceFilePath = "",
        [CallerMemberName] string callMemberName = ""
    )
    {
        var sourceFileName = sourceFilePath
            .Split(new[] { "\\", "/" }, StringSplitOptions.None)
            .Last()
            .Replace(".cs", "");

        var tablePrefix = ComputeSha256Hash(sourceFileName + "§" + callMemberName);
        var stateStore = new SqlServerStateStore(Sql.ConnectionString, tablePrefix);
        await stateStore.Initialize();

        // Clean up any existing data
        await using var conn = new SqlConnection(Sql.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand($"IF OBJECT_ID('{tablePrefix}_state', 'U') IS NOT NULL TRUNCATE TABLE {tablePrefix}_state", conn);
        await cmd.ExecuteNonQueryAsync();

        return stateStore;
    }

    private static string ComputeSha256Hash(string rawData)
    {
        using SHA256 sha256Hash = SHA256.Create();
        var bytes = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(rawData));

        var builder = new StringBuilder();
        foreach (var t in bytes)
            builder.Append(ReplaceDigits(t.ToString("X")));

        return builder.ToString();
    }

    private static string ReplaceDigits(string s)
        => s.Replace('0', 'a')
            .Replace('1', 'b')
            .Replace('2', 'c')
            .Replace('3', 'd')
            .Replace('4', 'e')
            .Replace('5', 'f')
            .Replace('6', 'g')
            .Replace('7', 'h')
            .Replace('8', 'i')
            .Replace('9', 'j');

    [TestMethod]
    public override Task GetAndReadReturnsEmptyDictionaryWhenNoStateExists()
        => GetAndReadReturnsEmptyDictionaryWhenNoStateExists<SqlServerStateStore, SqlServerStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, ids) => store.GetAndRead(ids)
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadReturnsSingleStateSuccessfully()
        => SetAndExecuteAndGetAndReadReturnsSingleStateSuccessfully<SqlServerStateStore, SqlServerStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new SqlServerStateStore.StoredState(id, pos, content),
            state => state.Id,
            state => state.Position,
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadReturnsMultipleStatesForSingleId()
        => SetAndExecuteAndGetAndReadReturnsMultipleStatesForSingleId<SqlServerStateStore, SqlServerStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new SqlServerStateStore.StoredState(id, pos, content),
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadReturnsMultipleIdsWithMultiplePositions()
        => SetAndExecuteAndGetAndReadReturnsMultipleIdsWithMultiplePositions<SqlServerStateStore, SqlServerStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new SqlServerStateStore.StoredState(id, pos, content),
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadHandlesNullContent()
        => SetAndExecuteAndGetAndReadHandlesNullContent<SqlServerStateStore, SqlServerStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new SqlServerStateStore.StoredState(id, pos, content),
            state => state.Id,
            state => state.Position,
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteUpdatesExistingState()
        => SetAndExecuteUpdatesExistingState<SqlServerStateStore, SqlServerStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new SqlServerStateStore.StoredState(id, pos, content),
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteHandlesEmptyDictionary()
        => SetAndExecuteHandlesEmptyDictionary<SqlServerStateStore, SqlServerStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids)
        );
}
