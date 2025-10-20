using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests;

[TestClass]
public class StateStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.StateStoreTests
{
    private static async Task<PostgreSqlStateStore> CreateAndInitializeStateStore(
        [CallerFilePath] string sourceFilePath = "",
        [CallerMemberName] string callMemberName = ""
    )
    {
        var sourceFileName = sourceFilePath
            .Split(new[] { "\\", "/" }, StringSplitOptions.None)
            .Last()
            .Replace(".cs", "");

        var tablePrefix = "t" + (sourceFileName + callMemberName).GenerateSHA256Hash()[..32];
        var stateStore = new PostgreSqlStateStore(Sql.ConnectionString, tablePrefix);
        await stateStore.Initialize();

        // Clean up any existing data
        await using var conn = new NpgsqlConnection(Sql.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand($"TRUNCATE TABLE {tablePrefix}_state", conn);
        await cmd.ExecuteNonQueryAsync();

        return stateStore;
    }

    [TestMethod]
    public override Task GetAndReadReturnsEmptyDictionaryWhenNoStateExists()
        => GetAndReadReturnsEmptyDictionaryWhenNoStateExists<PostgreSqlStateStore, PostgreSqlStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, ids) => store.GetAndRead(ids)
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadReturnsSingleStateSuccessfully()
        => SetAndExecuteAndGetAndReadReturnsSingleStateSuccessfully<PostgreSqlStateStore, PostgreSqlStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new PostgreSqlStateStore.StoredState(id, pos, content),
            state => state.Id,
            state => state.Position,
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadReturnsMultipleStatesForSingleId()
        => SetAndExecuteAndGetAndReadReturnsMultipleStatesForSingleId<PostgreSqlStateStore, PostgreSqlStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new PostgreSqlStateStore.StoredState(id, pos, content),
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadReturnsMultipleIdsWithMultiplePositions()
        => SetAndExecuteAndGetAndReadReturnsMultipleIdsWithMultiplePositions<PostgreSqlStateStore, PostgreSqlStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new PostgreSqlStateStore.StoredState(id, pos, content),
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadHandlesNullContent()
        => SetAndExecuteAndGetAndReadHandlesNullContent<PostgreSqlStateStore, PostgreSqlStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new PostgreSqlStateStore.StoredState(id, pos, content),
            state => state.Id,
            state => state.Position,
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteUpdatesExistingState()
        => SetAndExecuteUpdatesExistingState<PostgreSqlStateStore, PostgreSqlStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new PostgreSqlStateStore.StoredState(id, pos, content),
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteHandlesEmptyDictionary()
        => SetAndExecuteHandlesEmptyDictionary<PostgreSqlStateStore, PostgreSqlStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids)
        );
}
