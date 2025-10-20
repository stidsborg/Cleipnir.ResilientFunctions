using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests;

[TestClass]
public class StateStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.StateStoreTests
{
    private static async Task<MariaDbStateStore> CreateAndInitializeStateStore(
        [CallerFilePath] string sourceFilePath = "",
        [CallerMemberName] string callMemberName = ""
    )
    {
        var sourceFileName = sourceFilePath
            .Split(new[] { "\\", "/" }, StringSplitOptions.None)
            .Last()
            .Replace(".cs", "")
            .Replace(".", "_");

        var tablePrefix = "T" + Guid.NewGuid().ToString("N")[..25];
        var stateStore = new MariaDbStateStore(Sql.ConnectionString, tablePrefix);
        await stateStore.Initialize();

        // Clean up any existing data
        await using var conn = new MySqlConnection(Sql.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new MySqlCommand($"TRUNCATE TABLE {tablePrefix}_state", conn);
        await cmd.ExecuteNonQueryAsync();

        return stateStore;
    }

    [TestMethod]
    public override Task GetAndReadReturnsEmptyDictionaryWhenNoStateExists()
        => GetAndReadReturnsEmptyDictionaryWhenNoStateExists<MariaDbStateStore, MariaDbStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, ids) => store.GetAndRead(ids)
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadReturnsSingleStateSuccessfully()
        => SetAndExecuteAndGetAndReadReturnsSingleStateSuccessfully<MariaDbStateStore, MariaDbStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new MariaDbStateStore.StoredState(id, pos, content),
            state => state.Id,
            state => state.Position,
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadReturnsMultipleStatesForSingleId()
        => SetAndExecuteAndGetAndReadReturnsMultipleStatesForSingleId<MariaDbStateStore, MariaDbStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new MariaDbStateStore.StoredState(id, pos, content),
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadReturnsMultipleIdsWithMultiplePositions()
        => SetAndExecuteAndGetAndReadReturnsMultipleIdsWithMultiplePositions<MariaDbStateStore, MariaDbStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new MariaDbStateStore.StoredState(id, pos, content),
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteAndGetAndReadHandlesNullContent()
        => SetAndExecuteAndGetAndReadHandlesNullContent<MariaDbStateStore, MariaDbStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new MariaDbStateStore.StoredState(id, pos, content),
            state => state.Id,
            state => state.Position,
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteUpdatesExistingState()
        => SetAndExecuteUpdatesExistingState<MariaDbStateStore, MariaDbStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids),
            (id, pos, content) => new MariaDbStateStore.StoredState(id, pos, content),
            state => state.Content
        );

    [TestMethod]
    public override Task SetAndExecuteHandlesEmptyDictionary()
        => SetAndExecuteHandlesEmptyDictionary<MariaDbStateStore, MariaDbStateStore.StoredState>(
            CreateAndInitializeStateStore(),
            (store, states) => store.SetAndExecute(states),
            (store, ids) => store.GetAndRead(ids)
        );
}
