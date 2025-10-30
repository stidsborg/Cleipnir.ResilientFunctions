using System.Text;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class StateStoreTests 
{
    private const string TablePrefix = "test_state";

    private static SqlServerStateStore CreateStateStore()
    {
        var store = new SqlServerStateStore(Sql.ConnectionString, TablePrefix + "_" + System.Guid.NewGuid().ToString("N"));
        return store;
    }

    private static async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(Sql.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    private static async Task<int> ExecuteNonQuery(StoreCommand command)
    {
        await using var conn = await CreateConnection();
        await using var cmd = new SqlCommand(command.Sql, conn);
        foreach (var (value, name) in command.Parameters)
        {
            if (name != null)
                cmd.Parameters.AddWithValue(name, value);
            else
                cmd.Parameters.AddWithValue("", value);
        }
        return await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<SqlDataReader> ExecuteReader(StoreCommand command)
    {
        var conn = await CreateConnection();
        var cmd = new SqlCommand(command.Sql, conn);
        foreach (var (value, name) in command.Parameters)
        {
            if (name != null)
                cmd.Parameters.AddWithValue(name, value);
            else
                cmd.Parameters.AddWithValue("", value);
        }
        return await cmd.ExecuteReaderAsync();
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

        var id = TestStoredId.Create();
        var state = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("test content"), Version: 0);

        // Insert state
        var insertCommand = store.Insert(id, state);
        await ExecuteNonQuery(insertCommand);

        // Read state
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result.ShouldContainKey(id);
        result[id].ShouldContainKey(0);
        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("test content"));
        result[id][0].Version.ShouldBe(0);
    }

    [TestMethod]
    public async Task InsertMultipleStatesAtDifferentPositions()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        var state0 = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("content at 0"), Version: 0);
        var state5 = new SqlServerStateStore.StoredState(id, Position: 5, Content: Encoding.UTF8.GetBytes("content at 5"), Version: 0);
        var state10 = new SqlServerStateStore.StoredState(id, Position: 10, Content: Encoding.UTF8.GetBytes("content at 10"), Version: 0);

        await ExecuteNonQuery(store.Insert(id, state0));
        await ExecuteNonQuery(store.Insert(id, state5));
        await ExecuteNonQuery(store.Insert(id, state10));

        // Read all states
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

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

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();

        var state1 = new SqlServerStateStore.StoredState(id1, Position: 0, Content: Encoding.UTF8.GetBytes("id1 content"), Version: 0);
        var state2 = new SqlServerStateStore.StoredState(id2, Position: 0, Content: Encoding.UTF8.GetBytes("id2 content"), Version: 0);
        var state3 = new SqlServerStateStore.StoredState(id3, Position: 0, Content: Encoding.UTF8.GetBytes("id3 content"), Version: 0);

        await ExecuteNonQuery(store.Insert(id1, state1));
        await ExecuteNonQuery(store.Insert(id2, state2));
        await ExecuteNonQuery(store.Insert(id3, state3));

        // Read multiple IDs
        var getCommand = store.Get([id1, id2, id3]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

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

        var nonExistentId = TestStoredId.Create();

        var getCommand = store.Get([nonExistentId]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result.ShouldBeEmpty();
    }

    [TestMethod]
    public async Task UpdateStateIncrementsVersion()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        var initialState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("initial"), Version: 0);
        await ExecuteNonQuery(store.Insert(id, initialState));

        // Update the state
        var updatedState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("updated"), Version: 0);
        await ExecuteNonQuery(store.Update(id, updatedState));

        // Read state
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("updated"));
        result[id][0].Version.ShouldBe(1); // Version should be incremented
    }

    [TestMethod]
    public async Task UpdateWithWrongVersionDoesNotUpdate()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        var initialState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("initial"), Version: 0);
        await ExecuteNonQuery(store.Insert(id, initialState));

        // Try to update with wrong version
        var updatedState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("should not update"), Version: 999);
        await ExecuteNonQuery(store.Update(id, updatedState));

        // Read state - should still have original content
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("initial"));
        result[id][0].Version.ShouldBe(0); // Version should not change
    }

    [TestMethod]
    public async Task DeleteRemovesSpecificPositions()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        var state0 = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("content at 0"), Version: 0);
        var state5 = new SqlServerStateStore.StoredState(id, Position: 5, Content: Encoding.UTF8.GetBytes("content at 5"), Version: 0);
        var state10 = new SqlServerStateStore.StoredState(id, Position: 10, Content: Encoding.UTF8.GetBytes("content at 10"), Version: 0);

        await ExecuteNonQuery(store.Insert(id, state0));
        await ExecuteNonQuery(store.Insert(id, state5));
        await ExecuteNonQuery(store.Insert(id, state10));

        // Delete positions 0 and 10
        await ExecuteNonQuery(store.Delete(id, [0, 10]));

        // Read remaining states
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result[id].Count.ShouldBe(1);
        result[id].ShouldContainKey(5);
        result[id][5].Content.ShouldBe(Encoding.UTF8.GetBytes("content at 5"));
    }

    [TestMethod]
    public async Task DeleteWithEmptyPositionsListDoesNothing()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        var state = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("content"), Version: 0);
        await ExecuteNonQuery(store.Insert(id, state));

        // Delete with empty list - returns empty command
        var deleteCommand = store.Delete(id, []);
        deleteCommand.Sql.Trim().ShouldBe("SELECT;"); // Verify it returns empty SQL

        // Read state - should still exist since we didn't delete anything
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result[id].ShouldContainKey(0);
    }

    [TestMethod]
    public async Task AddTo0AppendsContentAndIncrementsVersion()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        var initialState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("initial"), Version: 0);
        await ExecuteNonQuery(store.Insert(id, initialState));

        // Append to position 0
        var appendState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes(" appended"), Version: 0);
        await ExecuteNonQuery(store.AddTo0(id, appendState));

        // Read state
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("initial appended"));
        result[id][0].Version.ShouldBe(1); // Version should be incremented
    }

    [TestMethod]
    public async Task AddTo0WithWrongVersionDoesNotAppend()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        var initialState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("initial"), Version: 0);
        await ExecuteNonQuery(store.Insert(id, initialState));

        // Try to append with wrong version
        var appendState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes(" should not append"), Version: 999);
        await ExecuteNonQuery(store.AddTo0(id, appendState));

        // Read state - should still have original content
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("initial"));
        result[id][0].Version.ShouldBe(0); // Version should not change
    }

    [TestMethod]
    public async Task EmptyContentCanBeReadBack()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        // Insert state with empty byte array
        var stateWithEmptyContent = new SqlServerStateStore.StoredState(id, Position: 0, Content: [], Version: 0);
        await ExecuteNonQuery(store.Insert(id, stateWithEmptyContent));

        // Read state
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result[id][0].Content.ShouldBe(new byte[0]);
        result[id][0].Version.ShouldBe(0);
    }

    [TestMethod]
    public async Task MultipleUpdatesIncrementVersionSequentially()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        // Insert initial state
        var state = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("v0"), Version: 0);
        await ExecuteNonQuery(store.Insert(id, state));

        // Update multiple times
        for (int i = 0; i < 5; i++)
        {
            var currentState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes($"v{i + 1}"), Version: i);
            await ExecuteNonQuery(store.Update(id, currentState));
        }

        // Read final state
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("v5"));
        result[id][0].Version.ShouldBe(5);
    }

    [TestMethod]
    public async Task StatesAreIsolatedByStoredId()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();

        var state1 = new SqlServerStateStore.StoredState(id1, Position: 0, Content: Encoding.UTF8.GetBytes("id1 content"), Version: 0);
        var state2 = new SqlServerStateStore.StoredState(id2, Position: 0, Content: Encoding.UTF8.GetBytes("id2 content"), Version: 0);

        await ExecuteNonQuery(store.Insert(id1, state1));
        await ExecuteNonQuery(store.Insert(id2, state2));

        // Delete from id1
        await ExecuteNonQuery(store.Delete(id1, [0]));

        // Read both - id2 should still exist
        var getCommand = store.Get([id1, id2]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result.ShouldNotContainKey(id1);
        result[id2][0].Content.ShouldBe(Encoding.UTF8.GetBytes("id2 content"));
    }

    [TestMethod]
    public async Task AppendCreatesNewStateAtNextPosition()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        // Insert initial states
        var state0 = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("position 0"), Version: 0);
        var state5 = new SqlServerStateStore.StoredState(id, Position: 5, Content: Encoding.UTF8.GetBytes("position 5"), Version: 0);
        await ExecuteNonQuery(store.Insert(id, state0));
        await ExecuteNonQuery(store.Insert(id, state5));

        // Append - should create at position MAX(5) + 3 = 8
        var appendState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("appended"), Version: 0);
        await ExecuteNonQuery(store.Append(id, appendState));

        // Read all states
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result[id].Count.ShouldBe(3);
        result[id][0].Content.ShouldBe(Encoding.UTF8.GetBytes("position 0"));
        result[id][5].Content.ShouldBe(Encoding.UTF8.GetBytes("position 5"));
        result[id][8].Content.ShouldBe(Encoding.UTF8.GetBytes("appended"));
    }

    [TestMethod]
    public async Task AppendToEmptyIdCreatesAtPosition2()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        // Append to empty ID - should create at position -1 + 3 = 2
        var appendState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("first append"), Version: 0);
        await ExecuteNonQuery(store.Append(id, appendState));

        // Read states
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        result[id].Count.ShouldBe(1);
        result[id][2].Content.ShouldBe(Encoding.UTF8.GetBytes("first append"));
    }

    [TestMethod]
    public async Task MultipleAppendsCreateSequentialPositions()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var id = TestStoredId.Create();

        // Append three times
        for (int i = 0; i < 3; i++)
        {
            var appendState = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes($"append {i}"), Version: 0);
            await ExecuteNonQuery(store.Append(id, appendState));
        }

        // Read all states
        var getCommand = store.Get([id]);
        await using var reader = await ExecuteReader(getCommand);
        var commandReader = new SqlServerStoreCommandReader(reader);
        var result = await store.Read(commandReader);

        // Should have positions: 2, 5, 8 (starting from -1, each adding 3)
        result[id].Count.ShouldBe(3);
        result[id][2].Content.ShouldBe(Encoding.UTF8.GetBytes("append 0"));
        result[id][5].Content.ShouldBe(Encoding.UTF8.GetBytes("append 1"));
        result[id][8].Content.ShouldBe(Encoding.UTF8.GetBytes("append 2"));
    }

    [TestMethod]
    public async Task BatchInsertMultipleStates()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new SqlServerCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        var state0 = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("batch content 0"), Version: 0);
        var state1 = new SqlServerStateStore.StoredState(id, Position: 1, Content: Encoding.UTF8.GetBytes("batch content 1"), Version: 0);
        var state2 = new SqlServerStateStore.StoredState(id, Position: 2, Content: Encoding.UTF8.GetBytes("batch content 2"), Version: 0);

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

        var executor = new SqlServerCommandExecutor(Sql.ConnectionString);
        var id = TestStoredId.Create();

        // First, insert initial states
        var state0 = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("initial 0"), Version: 0);
        var state1 = new SqlServerStateStore.StoredState(id, Position: 1, Content: Encoding.UTF8.GetBytes("initial 1"), Version: 0);
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state0)]));
        await executor.ExecuteNonQuery(new StoreCommands([store.Insert(id, state1)]));

        // Now batch: insert new state, update existing state, delete one state
        var state2 = new SqlServerStateStore.StoredState(id, Position: 2, Content: Encoding.UTF8.GetBytes("new state"), Version: 0);
        var updatedState0 = new SqlServerStateStore.StoredState(id, Position: 0, Content: Encoding.UTF8.GetBytes("updated 0"), Version: 0);

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
        result[id].ShouldContainKey(2);
        result[id][2].Content.ShouldBe(Encoding.UTF8.GetBytes("new state"));
        result[id].ShouldNotContainKey(1); // Deleted
    }

    [TestMethod]
    public async Task BatchReadMultipleIds()
    {
        var store = CreateStateStore();
        await store.Initialize();

        var executor = new SqlServerCommandExecutor(Sql.ConnectionString);

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();

        // Insert states for three different IDs
        await executor.ExecuteNonQuery(new StoreCommands([
            store.Insert(id1, new SqlServerStateStore.StoredState(id1, 0, Encoding.UTF8.GetBytes("id1 content"), 0)),
            store.Insert(id2, new SqlServerStateStore.StoredState(id2, 0, Encoding.UTF8.GetBytes("id2 content"), 0)),
            store.Insert(id3, new SqlServerStateStore.StoredState(id3, 0, Encoding.UTF8.GetBytes("id3 content"), 0))
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

        var executor = new SqlServerCommandExecutor(Sql.ConnectionString);

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();

        // Insert multiple states for multiple IDs in a single batch
        var commands = new StoreCommands([
            store.Insert(id1, new SqlServerStateStore.StoredState(id1, 0, Encoding.UTF8.GetBytes("id1 pos0"), 0)),
            store.Insert(id1, new SqlServerStateStore.StoredState(id1, 1, Encoding.UTF8.GetBytes("id1 pos1"), 0)),
            store.Insert(id2, new SqlServerStateStore.StoredState(id2, 0, Encoding.UTF8.GetBytes("id2 pos0"), 0)),
            store.Insert(id2, new SqlServerStateStore.StoredState(id2, 1, Encoding.UTF8.GetBytes("id2 pos1"), 0))
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

    // Helper class to wrap SqlDataReader as IStoreCommandReader
    private class SqlServerStoreCommandReader : IStoreCommandReader
    {
        private readonly SqlDataReader _reader;

        public SqlServerStoreCommandReader(SqlDataReader reader)
        {
            _reader = reader;
        }

        public int AffectedRows => _reader.RecordsAffected;
        public async Task<bool> ReadAsync() => await _reader.ReadAsync();
        public async Task<bool> MoveToNextResults() => await _reader.NextResultAsync();

        public Task<bool> IsDbNullAsync(int ordinal) => _reader.IsDBNullAsync(ordinal);
        public System.Guid GetGuid(int ordinal) => _reader.GetGuid(ordinal);
        public object GetValue(int ordinal) => _reader.GetValue(ordinal);
        public bool GetBoolean(int ordinal) => _reader.GetBoolean(ordinal);
        public int GetInt32(int ordinal) => _reader.GetInt32(ordinal);
        public long GetInt64(int ordinal) => _reader.GetInt64(ordinal);
        public string GetString(int ordinal) => _reader.GetString(ordinal);
        public bool IsDbNull(int ordinal) => _reader.IsDBNull(ordinal);

        public ValueTask DisposeAsync() => _reader.DisposeAsync();
    }
}
