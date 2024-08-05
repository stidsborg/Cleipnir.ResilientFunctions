using System;
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
        var functionId = TestFlowId.Create();
        var flowType = functionId.Type;
        const string timeoutId = "TimeoutId";
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        await store.UpsertTimeout(new StoredTimeout(functionId, timeoutId, expiry), overwrite: true);

        await BusyWait.Until(
            () => store.GetTimeouts(flowType.Value, expiry + 1).SelectAsync(ts => ts.Any())
        );
        
        var timeouts = await store.GetTimeouts(flowType.Value, expiresBefore: expiry + 1).ToListAsync();
        
        timeouts.Count.ShouldBe(1);
        timeouts[0].FlowId.ShouldBe(functionId);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].Expiry.ShouldBe(expiry);

        timeouts = await store.GetTimeouts(flowType.Value, expiresBefore: expiry - 1).ToListAsync();
        
        timeouts.ShouldBeEmpty();

        await store.RemoveTimeout(functionId, timeoutId);
        
        timeouts = await store
            .GetTimeouts(flowType.Value, expiresBefore: expiry + 1).ToListAsync();
        timeouts.ShouldBeEmpty();
    }
    
    public abstract Task ExistingTimeoutCanUpdatedSuccessfully();
    protected async Task ExistingTimeoutCanUpdatedSuccessfully(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var flowType = functionId.Type;
        const string timeoutId = "TimeoutId";
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        var storedTimeout = new StoredTimeout(functionId, timeoutId, expiry);
        await store.UpsertTimeout(storedTimeout, overwrite: true);

        await BusyWait.Until(
            () => store.GetTimeouts(flowType.Value, expiry + 1).SelectAsync(ts => ts.Any())
        );

        await store.UpsertTimeout(storedTimeout with { Expiry = 0 }, overwrite: true);

        var timeouts = await store.GetTimeouts(flowType.Value, expiry).ToListAsync();
        timeouts.Count.ShouldBe(1);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].FlowId.ShouldBe(functionId);
        timeouts[0].Expiry.ShouldBe(0);
    }
    
    public abstract Task OverwriteFalseDoesNotAffectExistingTimeout();
    protected async Task OverwriteFalseDoesNotAffectExistingTimeout(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var flowType = functionId.Type;
        const string timeoutId = "TimeoutId";
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        var storedTimeout = new StoredTimeout(functionId, timeoutId, expiry);
        await store.UpsertTimeout(storedTimeout, overwrite: true);

        await BusyWait.Until(
            () => store.GetTimeouts(flowType.Value, expiry + 1).SelectAsync(ts => ts.Any())
        );

        await store.UpsertTimeout(storedTimeout with { Expiry = 0 }, overwrite: false);

        var timeouts = await store.GetTimeouts(flowType.Value, expiry).ToListAsync();
        timeouts.Count.ShouldBe(1);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].FlowId.ShouldBe(functionId);
        timeouts[0].Expiry.ShouldBe(expiry);
    }
    
    public abstract Task RegisteredTimeoutIsReturnedFromTimeoutProvider();
    protected async Task RegisteredTimeoutIsReturnedFromTimeoutProvider(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();

        var timeoutProvider = new RegisteredRegisteredTimeouts(functionId, store);

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
        var functionId = TestFlowId.Create();

        var timeoutProvider = new RegisteredRegisteredTimeouts(functionId, store);

        var otherInstanceTimeoutProvider = new RegisteredRegisteredTimeouts(
            new FlowId(functionId.Type, functionId.Instance.Value + "2"), 
            store
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
        var functionId = TestFlowId.Create();

        var timeoutProvider = new RegisteredRegisteredTimeouts(functionId, store);

        await timeoutProvider.RegisterTimeout("timeoutId1", expiresIn: TimeSpan.FromHours(1));
        upsertCount.ShouldBe(1);

        var pendingTimeouts = await timeoutProvider.PendingTimeouts();
        pendingTimeouts.Count.ShouldBe(1);
        pendingTimeouts.Single().TimeoutId.ShouldBe("timeoutId1");

        await timeoutProvider.RegisterTimeout("timeoutId1", expiresIn: TimeSpan.FromHours(1));
        upsertCount.ShouldBe(1);
    }
    
    public abstract Task CancellingNonExistingTimeoutDoesNotResultInIO();
    protected async Task CancellingNonExistingTimeoutDoesNotResultInIO(Task<ITimeoutStore> storeTask)
    {
        var removeCount = 0;
        var store = new TimeoutStoreDecorator(await storeTask, removeTimeoutCallback: () => removeCount++);
        var functionId = TestFlowId.Create();

        var timeoutProvider = new RegisteredRegisteredTimeouts(functionId, store);
        
        var pendingTimeouts = await timeoutProvider.PendingTimeouts();
        pendingTimeouts.ShouldBeEmpty();

        await timeoutProvider.RegisterTimeout("SomeOtherTimeoutId", expiresIn: TimeSpan.FromHours(1));
        
        await timeoutProvider.CancelTimeout("SomeTimeoutId");
        
        removeCount.ShouldBe(0);
    }
    
    private class TimeoutStoreDecorator : ITimeoutStore
    {
        private readonly ITimeoutStore _inner;
        private readonly Action? _upsertTimeoutCallback;
        private readonly Action? _removeTimeoutCallback;

        public TimeoutStoreDecorator(
            ITimeoutStore inner, 
            Action? upsertTimeoutCallback = null, 
            Action? removeTimeoutCallback = null)
        {
            _inner = inner;
            _upsertTimeoutCallback = upsertTimeoutCallback;
            _removeTimeoutCallback = removeTimeoutCallback;
        }

        public Task Initialize() => _inner.Initialize();
        public Task Truncate() => _inner.Truncate();

        public Task UpsertTimeout(StoredTimeout storedTimeout, bool overwrite)
        {
            _upsertTimeoutCallback?.Invoke();
            return _inner.UpsertTimeout(storedTimeout, overwrite);
        }

        public Task RemoveTimeout(FlowId flowId, string timeoutId)
        {
            _removeTimeoutCallback?.Invoke();
            return _inner.RemoveTimeout(flowId, timeoutId);
        }
        
        public Task Remove(FlowId flowId)
            => _inner.Remove(flowId); 

        public Task<IEnumerable<StoredTimeout>> GetTimeouts(string flowType, long expiresBefore)
            => _inner.GetTimeouts(flowType, expiresBefore);

        public Task<IEnumerable<StoredTimeout>> GetTimeouts(FlowId flowId)
            => _inner.GetTimeouts(flowId);
    }
}