﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
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
        await store.UpsertTimeout(new StoredTimeout(functionId, timeoutId, expiry));

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
        await store.UpsertTimeout(storedTimeout);

        await BusyWait.Until(
            () => store.GetTimeouts(functionTypeId.Value, expiry + 1).SelectAsync(ts => ts.Any())
        );

        await store.UpsertTimeout(storedTimeout with { Expiry = 0 });

        var timeouts = await TaskLinq.ToListAsync(store.GetTimeouts(functionTypeId.Value, expiry));
        timeouts.Count.ShouldBe(1);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].FunctionId.ShouldBe(functionId);
        timeouts[0].Expiry.ShouldBe(0);
    }
    
    public abstract Task TimeoutStoreCanBeInitializedMultipleTimes();
    protected async Task TimeoutStoreCanBeInitializedMultipleTimes(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        await store.Initialize();
        await store.Initialize();
    }
}