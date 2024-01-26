using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Helpers;
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
        var functionId = TestFunctionId.Create();
        var functionTypeId = functionId.TypeId;
        const string timeoutId = "TimeoutId";
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        await store.UpsertTimeout(new StoredTimeout(functionId, timeoutId, expiry), overwrite: true);

        await BusyWait.Until(
            () => store.GetTimeouts(functionTypeId.Value, expiry + 1).SelectAsync(ts => ts.Any())
        );
        
        var timeouts = await TaskLinq.ToListAsync(store
                .GetTimeouts(functionTypeId.Value, expiresBefore: expiry + 1));
        
        timeouts.Count.ShouldBe(1);
        timeouts[0].FunctionId.ShouldBe(functionId);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].Expiry.ShouldBe(expiry);

        timeouts = await TaskLinq.ToListAsync(store
                .GetTimeouts(functionTypeId.Value, expiresBefore: expiry - 1));
        
        timeouts.ShouldBeEmpty();

        await store.RemoveTimeout(functionId, timeoutId);
        
        timeouts = await TaskLinq.ToListAsync(store
                .GetTimeouts(functionTypeId.Value, expiresBefore: expiry + 1));
        timeouts.ShouldBeEmpty();
    }
    
    public abstract Task ExistingTimeoutCanUpdatedSuccessfully();
    protected async Task ExistingTimeoutCanUpdatedSuccessfully(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var functionTypeId = functionId.TypeId;
        const string timeoutId = "TimeoutId";
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        var storedTimeout = new StoredTimeout(functionId, timeoutId, expiry);
        await store.UpsertTimeout(storedTimeout, overwrite: true);

        await BusyWait.Until(
            () => store.GetTimeouts(functionTypeId.Value, expiry + 1).SelectAsync(ts => ts.Any())
        );

        await store.UpsertTimeout(storedTimeout with { Expiry = 0 }, overwrite: true);

        var timeouts = await TaskLinq.ToListAsync(store.GetTimeouts(functionTypeId.Value, expiry));
        timeouts.Count.ShouldBe(1);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].FunctionId.ShouldBe(functionId);
        timeouts[0].Expiry.ShouldBe(0);
    }
    
    public abstract Task OverwriteFalseDoesNotAffectExistingTimeout();
    protected async Task OverwriteFalseDoesNotAffectExistingTimeout(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var functionTypeId = functionId.TypeId;
        const string timeoutId = "TimeoutId";
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        var storedTimeout = new StoredTimeout(functionId, timeoutId, expiry);
        await store.UpsertTimeout(storedTimeout, overwrite: true);

        await BusyWait.Until(
            () => store.GetTimeouts(functionTypeId.Value, expiry + 1).SelectAsync(ts => ts.Any())
        );

        await store.UpsertTimeout(storedTimeout with { Expiry = 0 }, overwrite: false);

        var timeouts = await store.GetTimeouts(functionTypeId.Value, expiry).ToListAsync();
        timeouts.Count.ShouldBe(1);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].FunctionId.ShouldBe(functionId);
        timeouts[0].Expiry.ShouldBe(expiry);
    }
    
    public abstract Task RegisteredTimeoutIsReturnedFromTimeoutProvider();
    protected async Task RegisteredTimeoutIsReturnedFromTimeoutProvider(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var timeoutProvider = new TimeoutProvider(
            functionId,
            store,
            messageWriter: null,
            timeoutCheckFrequency: TimeSpan.Zero
        );

        await timeoutProvider.RegisterTimeout("timeoutId1", expiresIn: TimeSpan.FromHours(1));
        await timeoutProvider.RegisterTimeout("timeoutId2", expiresIn: TimeSpan.FromHours(2));
        
        await BusyWait.Until(() => timeoutProvider.PendingTimeouts().SelectAsync(t => t.Count == 2));
       
        var timeouts = await timeoutProvider.PendingTimeouts();
        timeouts.Count.ShouldBe(2);
        timeouts.Any(t => t.TimeoutId == "timeoutId1").ShouldBe(true);
        timeouts.Any(t => t.TimeoutId == "timeoutId2").ShouldBe(true);
    }
    
    public abstract Task TimeoutStoreCanBeInitializedMultipleTimes();
    protected async Task TimeoutStoreCanBeInitializedMultipleTimes(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        await store.Initialize();
        await store.Initialize();
    }
}