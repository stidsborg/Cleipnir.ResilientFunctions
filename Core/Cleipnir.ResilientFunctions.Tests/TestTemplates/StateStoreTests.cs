using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class StateStoreTests
{
    public abstract Task GetAndReadReturnsEmptyDictionaryWhenNoStateExists();
    protected async Task GetAndReadReturnsEmptyDictionaryWhenNoStateExists<TStateStore, TStoredState>(
        Task<TStateStore> storeTask,
        Func<TStateStore, IReadOnlyList<StoredId>, Task<Dictionary<StoredId, Dictionary<long, TStoredState>>>> getAndRead
    )
    {
        var stateStore = await storeTask;
        var storedId = new StoredId(Guid.NewGuid());

        var result = await getAndRead(stateStore, new[] { storedId });

        result.ShouldNotBeNull();
        result.Count.ShouldBe(0);
    }

    public abstract Task SetAndExecuteAndGetAndReadReturnsSingleStateSuccessfully();
    protected async Task SetAndExecuteAndGetAndReadReturnsSingleStateSuccessfully<TStateStore, TStoredState>(
        Task<TStateStore> storeTask,
        Func<TStateStore, Dictionary<StoredId, Dictionary<long, TStoredState>>, Task> setAndExecute,
        Func<TStateStore, IReadOnlyList<StoredId>, Task<Dictionary<StoredId, Dictionary<long, TStoredState>>>> getAndRead,
        Func<StoredId, long, byte[], TStoredState> createState,
        Func<TStoredState, StoredId> getId,
        Func<TStoredState, long> getPosition,
        Func<TStoredState, byte[]> getContent
    )
    {
        var stateStore = await storeTask;
        var storedId = new StoredId(Guid.NewGuid());
        var content = new byte[] { 1, 2, 3, 4, 5 };
        var position = 0L;

        // Insert state using SetAndExecute
        await setAndExecute(stateStore, new Dictionary<StoredId, Dictionary<long, TStoredState>>
        {
            [storedId] = new() { [position] = createState(storedId, position, content) }
        });

        // Read state back using GetAndRead
        var result = await getAndRead(stateStore, new[] { storedId });

        result.ShouldNotBeNull();
        result.Count.ShouldBe(1);
        result.ContainsKey(storedId).ShouldBeTrue();
        result[storedId].Count.ShouldBe(1);
        result[storedId].ContainsKey(position).ShouldBeTrue();

        var storedState = result[storedId][position];
        getId(storedState).ShouldBe(storedId);
        getPosition(storedState).ShouldBe(position);
        getContent(storedState).ShouldBe(content);
    }

    public abstract Task SetAndExecuteAndGetAndReadReturnsMultipleStatesForSingleId();
    protected async Task SetAndExecuteAndGetAndReadReturnsMultipleStatesForSingleId<TStateStore, TStoredState>(
        Task<TStateStore> storeTask,
        Func<TStateStore, Dictionary<StoredId, Dictionary<long, TStoredState>>, Task> setAndExecute,
        Func<TStateStore, IReadOnlyList<StoredId>, Task<Dictionary<StoredId, Dictionary<long, TStoredState>>>> getAndRead,
        Func<StoredId, long, byte[], TStoredState> createState,
        Func<TStoredState, byte[]> getContent
    )
    {
        var stateStore = await storeTask;
        var storedId = new StoredId(Guid.NewGuid());
        var content1 = new byte[] { 1, 2, 3 };
        var content2 = new byte[] { 4, 5, 6 };
        var position1 = 0L;
        var position2 = 1L;

        // Insert multiple states for the same ID using SetAndExecute
        await setAndExecute(stateStore, new Dictionary<StoredId, Dictionary<long, TStoredState>>
        {
            [storedId] = new()
            {
                [position1] = createState(storedId, position1, content1),
                [position2] = createState(storedId, position2, content2)
            }
        });

        // Read states back using GetAndRead
        var result = await getAndRead(stateStore, new[] { storedId });

        result.ShouldNotBeNull();
        result.Count.ShouldBe(1);
        result.ContainsKey(storedId).ShouldBeTrue();
        result[storedId].Count.ShouldBe(2);

        getContent(result[storedId][position1]).ShouldBe(content1);
        getContent(result[storedId][position2]).ShouldBe(content2);
    }

    public abstract Task SetAndExecuteAndGetAndReadReturnsMultipleIdsWithMultiplePositions();
    protected async Task SetAndExecuteAndGetAndReadReturnsMultipleIdsWithMultiplePositions<TStateStore, TStoredState>(
        Task<TStateStore> storeTask,
        Func<TStateStore, Dictionary<StoredId, Dictionary<long, TStoredState>>, Task> setAndExecute,
        Func<TStateStore, IReadOnlyList<StoredId>, Task<Dictionary<StoredId, Dictionary<long, TStoredState>>>> getAndRead,
        Func<StoredId, long, byte[], TStoredState> createState,
        Func<TStoredState, byte[]> getContent
    )
    {
        var stateStore = await storeTask;
        var storedId1 = new StoredId(Guid.NewGuid());
        var storedId2 = new StoredId(Guid.NewGuid());
        var content1 = new byte[] { 1, 2, 3 };
        var content2 = new byte[] { 4, 5, 6 };
        var content3 = new byte[] { 7, 8, 9 };

        // Insert states for multiple IDs using SetAndExecute
        await setAndExecute(stateStore, new Dictionary<StoredId, Dictionary<long, TStoredState>>
        {
            [storedId1] = new()
            {
                [0L] = createState(storedId1, 0L, content1),
                [1L] = createState(storedId1, 1L, content2)
            },
            [storedId2] = new()
            {
                [0L] = createState(storedId2, 0L, content3)
            }
        });

        // Read states back using GetAndRead
        var result = await getAndRead(stateStore, new[] { storedId1, storedId2 });

        result.ShouldNotBeNull();
        result.Count.ShouldBe(2);
        result.ContainsKey(storedId1).ShouldBeTrue();
        result.ContainsKey(storedId2).ShouldBeTrue();
        result[storedId1].Count.ShouldBe(2);
        result[storedId2].Count.ShouldBe(1);

        getContent(result[storedId1][0L]).ShouldBe(content1);
        getContent(result[storedId1][1L]).ShouldBe(content2);
        getContent(result[storedId2][0L]).ShouldBe(content3);
    }

    public abstract Task SetAndExecuteAndGetAndReadHandlesNullContent();
    protected async Task SetAndExecuteAndGetAndReadHandlesNullContent<TStateStore, TStoredState>(
        Task<TStateStore> storeTask,
        Func<TStateStore, Dictionary<StoredId, Dictionary<long, TStoredState>>, Task> setAndExecute,
        Func<TStateStore, IReadOnlyList<StoredId>, Task<Dictionary<StoredId, Dictionary<long, TStoredState>>>> getAndRead,
        Func<StoredId, long, byte[], TStoredState> createState,
        Func<TStoredState, StoredId> getId,
        Func<TStoredState, long> getPosition,
        Func<TStoredState, byte[]> getContent
    )
    {
        var stateStore = await storeTask;
        var storedId = new StoredId(Guid.NewGuid());
        var position = 0L;

        // Insert state with null content using SetAndExecute
        await setAndExecute(stateStore, new Dictionary<StoredId, Dictionary<long, TStoredState>>
        {
            [storedId] = new()
            {
                [position] = createState(storedId, position, null)
            }
        });

        // Read state back using GetAndRead
        var result = await getAndRead(stateStore, new[] { storedId });

        result.ShouldNotBeNull();
        result.Count.ShouldBe(1);
        result.ContainsKey(storedId).ShouldBeTrue();
        result[storedId].Count.ShouldBe(1);

        var storedState = result[storedId][position];
        getId(storedState).ShouldBe(storedId);
        getPosition(storedState).ShouldBe(position);
        getContent(storedState).ShouldBeNull();
    }

    public abstract Task SetAndExecuteUpdatesExistingState();
    protected async Task SetAndExecuteUpdatesExistingState<TStateStore, TStoredState>(
        Task<TStateStore> storeTask,
        Func<TStateStore, Dictionary<StoredId, Dictionary<long, TStoredState>>, Task> setAndExecute,
        Func<TStateStore, IReadOnlyList<StoredId>, Task<Dictionary<StoredId, Dictionary<long, TStoredState>>>> getAndRead,
        Func<StoredId, long, byte[], TStoredState> createState,
        Func<TStoredState, byte[]> getContent
    )
    {
        var stateStore = await storeTask;
        var storedId = new StoredId(Guid.NewGuid());
        var position = 0L;
        var initialContent = new byte[] { 1, 2, 3 };
        var updatedContent = new byte[] { 4, 5, 6, 7 };

        // Insert initial state
        await setAndExecute(stateStore, new Dictionary<StoredId, Dictionary<long, TStoredState>>
        {
            [storedId] = new()
            {
                [position] = createState(storedId, position, initialContent)
            }
        });

        // Update the same state
        await setAndExecute(stateStore, new Dictionary<StoredId, Dictionary<long, TStoredState>>
        {
            [storedId] = new()
            {
                [position] = createState(storedId, position, updatedContent)
            }
        });

        // Read state back - should have updated content
        var result = await getAndRead(stateStore, new[] { storedId });

        result.ShouldNotBeNull();
        result.Count.ShouldBe(1);
        result[storedId].Count.ShouldBe(1);
        getContent(result[storedId][position]).ShouldBe(updatedContent);
    }

    public abstract Task SetAndExecuteHandlesEmptyDictionary();
    protected async Task SetAndExecuteHandlesEmptyDictionary<TStateStore, TStoredState>(
        Task<TStateStore> storeTask,
        Func<TStateStore, Dictionary<StoredId, Dictionary<long, TStoredState>>, Task> setAndExecute,
        Func<TStateStore, IReadOnlyList<StoredId>, Task<Dictionary<StoredId, Dictionary<long, TStoredState>>>> getAndRead
    )
    {
        var stateStore = await storeTask;

        // Should not throw when called with empty dictionary
        await setAndExecute(stateStore, new Dictionary<StoredId, Dictionary<long, TStoredState>>());

        // Verify no data was added
        var storedId = new StoredId(Guid.NewGuid());
        var result = await getAndRead(stateStore, new[] { storedId });

        result.ShouldNotBeNull();
        result.Count.ShouldBe(0);
    }
}
