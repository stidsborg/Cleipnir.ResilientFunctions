﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
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
        await store.UpsertTimeout(new StoredTimeout(functionId, timeoutId, expiry), overwrite: true);

        await BusyWait.Until(
            () => store.GetTimeouts(functionTypeId.Value, expiry + 1).SelectAsync(ts => ts.Any())
        );
        
        var timeouts = await store.GetTimeouts(functionTypeId.Value, expiresBefore: expiry + 1).ToListAsync();
        
        timeouts.Count.ShouldBe(1);
        timeouts[0].FunctionId.ShouldBe(functionId);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].Expiry.ShouldBe(expiry);

        timeouts = await store.GetTimeouts(functionTypeId.Value, expiresBefore: expiry - 1).ToListAsync();
        
        timeouts.ShouldBeEmpty();

        await store.RemoveTimeout(functionId, timeoutId);
        
        timeouts = await store
            .GetTimeouts(functionTypeId.Value, expiresBefore: expiry + 1).ToListAsync();
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

        var timeouts = await store.GetTimeouts(functionTypeId.Value, expiry).ToListAsync();
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
    
    public abstract Task RegisteredTimeoutIsReturnedFromTimeoutProviderForFunctionId();
    protected async Task RegisteredTimeoutIsReturnedFromTimeoutProviderForFunctionId(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var timeoutProvider = new TimeoutProvider(
            functionId,
            store,
            messageWriter: null,
            timeoutCheckFrequency: TimeSpan.Zero
        );

        var otherInstanceTimeoutProvider = new TimeoutProvider(
            new FunctionId(functionId.TypeId, functionId.InstanceId.Value + "2"),
            store,
            messageWriter: null,
            timeoutCheckFrequency: TimeSpan.Zero
        );

        await timeoutProvider.RegisterTimeout("timeoutId1", expiresIn: TimeSpan.FromHours(1));
        await timeoutProvider.RegisterTimeout("timeoutId2", expiresIn: TimeSpan.FromHours(2));
        await otherInstanceTimeoutProvider.RegisterTimeout("timeoutId3", expiresIn: TimeSpan.FromHours(3));
        
        await BusyWait.Until(() => timeoutProvider.PendingTimeouts().SelectAsync(t => t.Count == 2));
       
        var timeouts = await timeoutProvider.PendingTimeouts();
        timeouts.Count.ShouldBe(2);
        timeouts.Any(t => t.TimeoutId == "timeoutId1").ShouldBe(true);
        timeouts.Any(t => t.TimeoutId == "timeoutId2").ShouldBe(true);
    }
    
    public abstract Task TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout();
    protected async Task TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout(Task<ITimeoutStore> storeTask)
    {
        var upsertCount = 0;
        var store = new TimeoutStoreDecorator(await storeTask, () => upsertCount++);
        var functionId = TestFunctionId.Create();

        var timeoutProvider = new TimeoutProvider(
            functionId,
            store,
            messageWriter: null,
            timeoutCheckFrequency: TimeSpan.Zero
        );

        await timeoutProvider.RegisterTimeout("timeoutId1", expiresIn: TimeSpan.FromHours(1));
        upsertCount.ShouldBe(1);

        var pendingTimeouts = await timeoutProvider.PendingTimeouts();
        pendingTimeouts.Count.ShouldBe(1);
        pendingTimeouts.Single().TimeoutId.ShouldBe("timeoutId1");

        await timeoutProvider.RegisterTimeout("timeoutId1", expiresIn: TimeSpan.FromHours(1), overwrite: false);
        upsertCount.ShouldBe(1);
    }

    private class TimeoutStoreDecorator : ITimeoutStore
    {
        private readonly ITimeoutStore _inner;
        private readonly Action _upsertTimeoutCallback;

        public TimeoutStoreDecorator(ITimeoutStore inner, Action upsertTimeoutCallback)
        {
            _inner = inner;
            _upsertTimeoutCallback = upsertTimeoutCallback;
        }

        public Task Initialize() => _inner.Initialize();
        public Task Truncate() => _inner.Truncate();

        public Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite)
        {
            _upsertTimeoutCallback();
            return _inner.UpsertTimeout(storedTimeout, overwrite);
        }

        public Task RemoveTimeout(FunctionId functionId, string timeoutId)
            => _inner.RemoveTimeout(functionId, timeoutId);

        public Task Remove(FunctionId functionId)
            => _inner.Remove(functionId); 

        public Task<IEnumerable<StoredTimeout>> GetTimeouts(string functionTypeId, long expiresBefore)
            => _inner.GetTimeouts(functionTypeId, expiresBefore);

        public Task<IEnumerable<StoredTimeout>> GetTimeouts(FunctionId functionId)
            => _inner.GetTimeouts(functionId);
    }
}