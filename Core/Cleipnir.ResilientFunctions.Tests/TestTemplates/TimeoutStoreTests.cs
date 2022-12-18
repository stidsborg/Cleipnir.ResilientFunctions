using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class TimeoutStoreTests
{
    public abstract Task TimeoutCanBeCreatedFetchedAndRemoveSuccessfully();
    protected async Task TimeoutCanBeCreatedFetchedAndRemoveSuccessfully(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var functionId = new FunctionId(
            nameof(TimeoutCanBeCreatedFetchedAndRemoveSuccessfully),
            "InstanceId"
        );
        var (functionTypeId, functionInstanceId) = functionId;
        const string timeoutId = "TimeoutId";
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        await store.UpsertTimeout(new StoredTimeout(functionId, timeoutId, expiry));

        var timeouts = await store
            .GetTimeouts(functionTypeId.Value, expiresBefore: expiry + 1)
            .ToListAsync();
        
        timeouts.Count.ShouldBe(1);
        timeouts[0].FunctionId.ShouldBe(functionId);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].Expiry.ShouldBe(expiry);
        
        timeouts = await store
            .GetTimeouts(functionTypeId.Value, expiresBefore: expiry - 1)
            .ToListAsync();
        
        timeouts.ShouldBeEmpty();

        await store.RemoveTimeout(functionId, timeoutId);
        
        timeouts = await store
            .GetTimeouts(functionTypeId.Value, expiresBefore: expiry + 1)
            .ToListAsync();
        timeouts.ShouldBeEmpty();
    }
    
    public abstract Task TimeoutStoreCanBeInitializedMultipleTimes();
    protected async Task TimeoutStoreCanBeInitializedMultipleTimes(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        await store.Initialize();
        await store.Initialize();
    }
}