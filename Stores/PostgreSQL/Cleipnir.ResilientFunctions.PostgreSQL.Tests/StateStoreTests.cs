using System.Text;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests;

[TestClass]
public class StateStoreTests
{
    private const string TablePrefix = "test_state";

    private static PostgreSqlStateStore CreateStateStore()
    {
        var store = new PostgreSqlStateStore(Sql.ConnectionString, TablePrefix + "_" + System.Guid.NewGuid().ToString("N"));
        return store;
    }

    [TestMethod]
    public async Task InitializeCreatesTable()
    {
        var store = CreateStateStore();

        // Should not throw
        await store.Initialize();

        // Should be idempotent
        await store.Initialize();
    }

    [TestMethod]
    public async Task InsertAndReadSingleState()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();
        var state = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("test content"), Version: 0);

        // Insert state
        var insertCommand = store.Insert(id, state);
        await executor.ExecuteNonQuery(new StoreCommands([insertCommand]));

        // Read state
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result.ShouldContainKey(id);
        result[id].ShouldContainKey(0L);
        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("test content"));
        result[id][0].Version.ShouldBe(0);
    }

    [TestMethod]
    public async Task InsertMultipleStatesAtDifferentPositions()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        var state0 = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("content at 0"), Version: 0);
        var state5 = new PostgreSqlStateStore.StoredState(id, Position: 5, Content: Encoding.UTF8.GetBytes("content at 5"), Version: 0);
        var state10 = new PostgreSqlStateStore.StoredState(id, Position: 10, Content: Encoding.UTF8.GetBytes("content at 10"), Version: 0);

        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state0)]));
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state5)]));
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state10)]));

        // Read all states
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id].Count.ShouldBe(3);
        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("content at 0"));
        result[id][5].Content.ShouldBe(Encoding.UTF8.GetBytes("content at 5"));
        result[id][10].Content.ShouldBe(Encoding.UTF8.GetBytes("content at 10"));
    }

    [TestMethod]
    public async Task GetMultipleIdsReturnsAllStates()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();

        var state1 = new PostgreSqlStateStore.StoredState(id1, Position: 0, Content: Encoding.UTF8.GetBytes("id1 content"), Version: 0);
        var state2 = new PostgreSqlStateStore.StoredState(id2, Position: 0, Content: Encoding.UTF8.GetBytes("id2 content"), Version: 0);
        var state3 = new PostgreSqlStateStore.StoredState(id3, Position: 0, Content: Encoding.UTF8.GetBytes("id3 content"), Version: 0);

        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id1, state1)]));
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id2, state2)]));
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id3, state3)]));

        // Read multiple IDs
        var getCommand = store.Get([id1, id2, id3]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result.Count.ShouldBe(3);
        result[id1][0].Content.ShouldBe(Encoding.UTF8.GetBytes("id1 content"));
        result[id2][0].Content.ShouldBe(Encoding.UTF8.GetBytes("id2 content"));
        result[id3][0].Content.ShouldBe(Encoding.UTF8.GetBytes("id3 content"));
    }

    [TestMethod]
    public async Task GetNonExistentIdReturnsEmptyResult()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var nonExistentId = TestStoredId.Create();

        var getCommand = store.Get([nonExistentId]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result.ShouldBeEmpty();
    }

    [TestMethod]
    public async Task UpdateStateIncrementsVersion()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        var initialState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("initial"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, initialState)]));

        // Update the state
        var updatedState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("updated"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Update(id, updatedState)]));

        // Read state
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("updated"));
        result[id][0].Version.ShouldBe(1); // Version should be incremented
    }

    [TestMethod]
    public async Task UpdateWithWrongVersionDoesNotUpdate()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        var initialState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("initial"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, initialState)]));

        // Try to update with wrong version
        var updatedState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("should not update"), Version: 999);
        await executor.ExecuteNonQuery(new StoreCommands([store.Update(id, updatedState)]));

        // Read state - should still have original content
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("initial"));
        result[id][0].Version.ShouldBe(0); // Version should not change
    }

    [TestMethod]
    public async Task DeleteRemovesSpecificPositions()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        var state0 = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("content at 0"), Version: 0);
        var state5 = new PostgreSqlStateStore.StoredState(id, Position: 5, Content: Encoding.UTF8.GetBytes("content at 5"), Version: 0);
        var state10 = new PostgreSqlStateStore.StoredState(id, Position: 10, Content: Encoding.UTF8.GetBytes("content at 10"), Version: 0);

        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state0)]));
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state5)]));
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state10)]));

        // Delete positions 0 and 10
        await executor.ExecuteNonQuery(new StoreCommands([store.Delete(id, [0L, 10L])]));

        // Read remaining states
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id].Count.ShouldBe(1);
        result[id].ShouldContainKey(5L);
        result[id][5].Content.ShouldBe(Encoding.UTF8.GetBytes("content at 5"));
    }

    [TestMethod]
    public async Task DeleteWithEmptyPositionsListDoesNothing()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        var state = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("content"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state)]));

        // Delete with empty list - returns no-op SELECT command
        var deleteCommand = store.Delete(id, []);
        deleteCommand.Sql.Trim().ShouldBe("SELECT"); // Verify it returns a no-op command

        // Read state - should still exist since we didn't delete anything
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id].ShouldContainKey(0L);
    }

    [TestMethod]
    public async Task AddTo0AppendsContentAndIncrementsVersion()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        var initialState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("initial"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, initialState)]));

        // Append to position 0
        var appendState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes(" appended"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.AddTo0(id, appendState)]));

        // Read state
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("initial appended"));
        result[id][0].Version.ShouldBe(1); // Version should be incremented
    }

    [TestMethod]
    public async Task AddTo0WithWrongVersionDoesNotAppend()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        var initialState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("initial"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, initialState)]));

        // Try to append with wrong version
        var appendState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes(" should not append"), Version: 999);
        await executor.ExecuteNonQuery(new StoreCommands([store.AddTo0(id, appendState)]));

        // Read state - should still have original content
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("initial"));
        result[id][0].Version.ShouldBe(0); // Version should not change
    }

    [TestMethod]
    public async Task NullContentCanBeReadBack()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        // Insert state with empty byte array instead of null
        var stateWithEmptyContent = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: [], Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, stateWithEmptyContent)]));

        // Read state
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id][0].Content.ShouldBe(new byte[0]);
        result[id][0].Version.ShouldBe(0);
    }

    [TestMethod]
    public async Task MultipleUpdatesIncrementVersionSequentially()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        // Insert initial state
        var state = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("v0"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state)]));

        // Update multiple times
        for (int i = 0; i < 5; i++)
        {
            var currentState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes($"v{i + 1}"), Version: i);
            await executor.ExecuteNonQuery(new StoreCommands([store.Update(id, currentState)]));
        }

        // Read final state
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("v5"));
        result[id][0].Version.ShouldBe(5);
    }

    [TestMethod]
    public async Task StatesAreIsolatedByStoredId()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();

        var state1 = new PostgreSqlStateStore.StoredState(id1, Position: 0, Content: Encoding.UTF8.GetBytes("id1 content"), Version: 0);
        var state2 = new PostgreSqlStateStore.StoredState(id2, Position: 0, Content: Encoding.UTF8.GetBytes("id2 content"), Version: 0);

        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id1, state1)]));
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id2, state2)]));

        // Delete from id1
        await executor.ExecuteNonQuery(new StoreCommands([store.Delete(id1, [0L])]));

        // Read both - id2 should still exist
        var getCommand = store.Get([id1, id2]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result.ShouldNotContainKey(id1);
        result[id2][0].Content.ShouldBe(Encoding.UTF8.GetBytes("id2 content"));
    }

    [TestMethod]
    public async Task AppendCreatesNewStateAtNextPosition()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        // Insert initial states
        var state0 = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("position 0"), Version: 0);
        var state5 = new PostgreSqlStateStore.StoredState(id, Position: 5, Content: Encoding.UTF8.GetBytes("position 5"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state0)]));
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state5)]));

        // Append - should create at position MAX(5) + 1 = 6
        var appendState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("appended"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Append(id, appendState)]));

        // Read all states
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id].Count.ShouldBe(3);
        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("position 0"));
        result[id][5].Content.ShouldBe(Encoding.UTF8.GetBytes("position 5"));
        result[id][6].Content.ShouldBe(Encoding.UTF8.GetBytes("appended"));
    }

    [TestMethod]
    public async Task AppendToEmptyIdCreatesAtPosition2()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        // Append to empty ID - should create at position -1 + 3 = 2
        var appendState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("first append"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Append(id, appendState)]));

        // Read states
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id].Count.ShouldBe(1);
        result[id][2].Content.ShouldBe(Encoding.UTF8.GetBytes("first append"));
    }

    [TestMethod]
    public async Task MultipleAppendsCreateSequentialPositions()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        // Append three times
        for (int i = 0; i < 3; i++)
        {
            var appendState = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes($"append {i}"), Version: 0);
            await executor.ExecuteNonQuery(new StoreCommands([store.Append(id, appendState)]));
        }

        // Read all states
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        // Should have positions: 2, 3, 4 (starting from 1, each adding 1)
        result[id].Count.ShouldBe(3);
        result[id][2].Content.ShouldBe(Encoding.UTF8.GetBytes("append 0"));
        result[id][3].Content.ShouldBe(Encoding.UTF8.GetBytes("append 1"));
        result[id][4].Content.ShouldBe(Encoding.UTF8.GetBytes("append 2"));
    }

    [TestMethod]
    public async Task BatchInsertMultipleStates()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        var state0 = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("batch content 0"), Version: 0);
        var state1 = new PostgreSqlStateStore.StoredState(id, Position: 1, Content: Encoding.UTF8.GetBytes("batch content 1"), Version: 0);
        var state2 = new PostgreSqlStateStore.StoredState(id, Position: 2, Content: Encoding.UTF8.GetBytes("batch content 2"), Version: 0);

        // Insert all three states in a single batch
        var commands = new StoreCommands([
            store.Insert(id, state0),
            store.Insert(id, state1),
            store.Insert(id, state2)
        ]);
        await executor.ExecuteNonQuery(commands);

        // Read all states
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id].Count.ShouldBe(3);
        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("batch content 0"));
        result[id][1].Content.ShouldBe(Encoding.UTF8.GetBytes("batch content 1"));
        result[id][2].Content.ShouldBe(Encoding.UTF8.GetBytes("batch content 2"));
    }

    [TestMethod]
    public async Task BatchMixedOperations()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        // First, insert initial states
        var state0 = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("initial 0"), Version: 0);
        var state1 = new PostgreSqlStateStore.StoredState(id, Position: 1, Content: Encoding.UTF8.GetBytes("initial 1"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state0)]));
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state1)]));

        // Now batch: insert new state, update existing state, delete one state
        var state2 = new PostgreSqlStateStore.StoredState(id, Position: 2, Content: Encoding.UTF8.GetBytes("new state"), Version: 0);
        var updatedState0 = new PostgreSqlStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("updated 0"), Version: 0);

        var commands = new StoreCommands([
            store.Insert(id, state2),
            store.Update(id, updatedState0),
            store.Delete(id, [1])
        ]);
        await executor.ExecuteNonQuery(commands);

        // Read all states
        var getCommand = store.Get([id]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result[id].Count.ShouldBe(2);
        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("updated 0"));
        result[id][0].Version.ShouldBe(1); // Version incremented by update
        result[id].ShouldContainKey(2L);
        result[id][2].Content.ShouldBe(Encoding.UTF8.GetBytes("new state"));
        result[id].ShouldNotContainKey(1L); // Deleted
    }

    [TestMethod]
    public async Task BatchReadMultipleIds()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();

        // Insert states for three different IDs
        await executor.ExecuteNonQuery(new StoreCommands([
            store.Insert(id1, new PostgreSqlStateStore.StoredState(id1, 0, Encoding.UTF8.GetBytes("id1 content"), 0)),
            store.Insert(id2, new PostgreSqlStateStore.StoredState(id2, 0, Encoding.UTF8.GetBytes("id2 content"), 0)),
            store.Insert(id3, new PostgreSqlStateStore.StoredState(id3, 0, Encoding.UTF8.GetBytes("id3 content"), 0))
        ]));

        // Read all three IDs in a single query
        var getCommand = store.Get([id1, id2, id3]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result.Count.ShouldBe(3);
        result[id1][0].Content.ShouldBe(Encoding.UTF8.GetBytes("id1 content"));
        result[id2][0].Content.ShouldBe(Encoding.UTF8.GetBytes("id2 content"));
        result[id3][0].Content.ShouldBe(Encoding.UTF8.GetBytes("id3 content"));
    }

    [TestMethod]
    public async Task BatchInsertAcrossMultipleIds()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new PostgresCommandExecutor(Sql.ConnectionString);

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();

        // Insert multiple states for multiple IDs in a single batch
        var commands = new StoreCommands([
            store.Insert(id1, new PostgreSqlStateStore.StoredState(id1, 0, Encoding.UTF8.GetBytes("id1 pos0"), 0)),
            store.Insert(id1, new PostgreSqlStateStore.StoredState(id1, 1, Encoding.UTF8.GetBytes("id1 pos1"), 0)),
            store.Insert(id2, new PostgreSqlStateStore.StoredState(id2, 0, Encoding.UTF8.GetBytes("id2 pos0"), 0)),
            store.Insert(id2, new PostgreSqlStateStore.StoredState(id2, 1, Encoding.UTF8.GetBytes("id2 pos1"), 0))
        ]);
        await executor.ExecuteNonQuery(commands);

        // Read both IDs
        var getCommand = store.Get([id1, id2]);
        var reader = await executor.Execute(new StoreCommands([getCommand]));
        var result = await store.Read(reader);

        result.Count.ShouldBe(2);
        result[id1].Count.ShouldBe(2);
        result[id1][0].Content.ShouldBe(Encoding.UTF8.GetBytes("id1 pos0"));
        result[id1][1].Content.ShouldBe(Encoding.UTF8.GetBytes("id1 pos1"));
        result[id2].Count.ShouldBe(2);
        result[id2][0].Content.ShouldBe(Encoding.UTF8.GetBytes("id2 pos0"));
        result[id2][1].Content.ShouldBe(Encoding.UTF8.GetBytes("id2 pos1"));
    }
}
