using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
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
        var functionId = TestStoredId.Create();
        var timeoutId = "TimeoutId".ToEffectId();
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        await store.UpsertTimeout(new StoredTimeout(functionId, timeoutId, expiry), overwrite: true);

        await BusyWait.Until(
            () => store.GetTimeouts(expiry + 1).SelectAsync(ts => ts.Any())
        );
        
        var timeouts = await store.GetTimeouts(expiresBefore: expiry + 1).ToListAsync();
        
        timeouts.Count.ShouldBe(1);
        timeouts[0].StoredId.ShouldBe(functionId);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].Expiry.ShouldBe(expiry);

        timeouts = await store.GetTimeouts(expiresBefore: expiry - 1).ToListAsync();
        
        timeouts.ShouldBeEmpty();

        await store.RemoveTimeout(functionId, timeoutId);
        
        timeouts = await store
            .GetTimeouts(expiresBefore: expiry + 1).ToListAsync();
        timeouts.ShouldBeEmpty();
    }
    
    public abstract Task ExistingTimeoutCanUpdatedSuccessfully();
    protected async Task ExistingTimeoutCanUpdatedSuccessfully(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create(); 
        var timeoutId = "TimeoutId".ToEffectId();
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        var storedTimeout = new StoredTimeout(functionId, timeoutId, expiry);
        await store.UpsertTimeout(storedTimeout, overwrite: true);

        await BusyWait.Until(
            () => store.GetTimeouts(expiry + 1).SelectAsync(ts => ts.Any())
        );

        await store.UpsertTimeout(storedTimeout with { Expiry = 0 }, overwrite: true);

        var timeouts = await store.GetTimeouts(expiry).ToListAsync();
        timeouts.Count.ShouldBe(1);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].StoredId.ShouldBe(functionId);
        timeouts[0].Expiry.ShouldBe(0);
    }
    
    public abstract Task OverwriteFalseDoesNotAffectExistingTimeout();
    protected async Task OverwriteFalseDoesNotAffectExistingTimeout(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        var timeoutId = "TimeoutId".ToEffectId();
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        var storedTimeout = new StoredTimeout(functionId, timeoutId, expiry);
        await store.UpsertTimeout(storedTimeout, overwrite: true);

        await BusyWait.Until(
            () => store.GetTimeouts(expiry + 1).SelectAsync(ts => ts.Any())
        );

        await store.UpsertTimeout(storedTimeout with { Expiry = 0 }, overwrite: false);

        var timeouts = await store.GetTimeouts(expiry).ToListAsync();
        timeouts.Count.ShouldBe(1);
        timeouts[0].TimeoutId.ShouldBe(timeoutId);
        timeouts[0].StoredId.ShouldBe(functionId);
        timeouts[0].Expiry.ShouldBe(expiry);
    }
    
    public abstract Task RegisteredTimeoutIsReturnedFromRegisteredTimeouts();
    protected async Task RegisteredTimeoutIsReturnedFromRegisteredTimeouts(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        
        var registeredTimeouts = new RegisteredTimeouts(storedId, store.TimeoutStore, CreateEffect(flowId, storedId, store), () => DateTime.UtcNow);

        await registeredTimeouts.RegisterTimeout("timeoutId1".ToEffectId(), expiresIn: TimeSpan.FromHours(1));
        await registeredTimeouts.RegisterTimeout("timeoutId2".ToEffectId(), expiresIn: TimeSpan.FromHours(2));
        
        await BusyWait.Until(() => registeredTimeouts.PendingTimeouts().SelectAsync(t => t.Count == 2));
       
        var timeouts = await registeredTimeouts.PendingTimeouts();
        timeouts.Count.ShouldBe(2);
        timeouts.Any(t => t.TimeoutId == "timeoutId1".ToEffectId()).ShouldBe(true);
        timeouts.Any(t => t.TimeoutId == "timeoutId2".ToEffectId()).ShouldBe(true);
    }
    
    public abstract Task TimeoutStoreCanBeInitializedMultipleTimes();
    protected async Task TimeoutStoreCanBeInitializedMultipleTimes(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        await store.Initialize();
        await store.Initialize();
    }
    
    public abstract Task RegisteredTimeoutIsReturnedFromRegisteredTimeoutsForFunctionId();
    protected async Task RegisteredTimeoutIsReturnedFromRegisteredTimeoutsForFunctionId(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));

        var effect = CreateEffect(flowId, storedId, store);
        var registeredTimeouts = new RegisteredTimeouts(storedId, store.TimeoutStore, effect, () => DateTime.UtcNow);

        var otherInstanceRegisteredTimeouts = new RegisteredTimeouts(
            storedId with { Instance = (storedId.Instance + "2").ToStoredInstance() }, 
            store.TimeoutStore,
            effect,
            () => DateTime.UtcNow
        );

        await registeredTimeouts.RegisterTimeout("timeoutId1".ToEffectId(), expiresIn: TimeSpan.FromHours(1));
        await registeredTimeouts.RegisterTimeout("timeoutId2".ToEffectId(), expiresIn: TimeSpan.FromHours(2));
        await otherInstanceRegisteredTimeouts.RegisterTimeout("timeoutId3".ToEffectId(), expiresIn: TimeSpan.FromHours(3));
        
        await BusyWait.Until(() => registeredTimeouts.PendingTimeouts().SelectAsync(t => t.Count == 2));
       
        var timeouts = await registeredTimeouts.PendingTimeouts();
        timeouts.Count.ShouldBe(2);
        timeouts.Any(t => t.TimeoutId == "timeoutId1".ToEffectId()).ShouldBe(true);
        timeouts.Any(t => t.TimeoutId == "timeoutId2".ToEffectId()).ShouldBe(true);
    }
    
    public abstract Task TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout();
    protected async Task TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout(Task<IFunctionStore> storeTask)
    {
        var upsertCount = 0;
        var store = new TimeoutStoreDecorator((await storeTask).TimeoutStore, () => upsertCount++);
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));

        var registeredTimeouts = new RegisteredTimeouts(storedId, store, CreateEffect(flowId, storedId, await storeTask), () => DateTime.UtcNow);

        await registeredTimeouts.RegisterTimeout("timeoutId1".ToEffectId(), expiresIn: TimeSpan.FromHours(1));
        upsertCount.ShouldBe(1);

        var pendingTimeouts = await registeredTimeouts.PendingTimeouts();
        pendingTimeouts.Count.ShouldBe(1);
        pendingTimeouts.Single().TimeoutId.ShouldBe("timeoutId1".ToEffectId());

        await registeredTimeouts.RegisterTimeout("timeoutId1".ToEffectId(), expiresIn: TimeSpan.FromHours(1));
        upsertCount.ShouldBe(1);
    }
    
    public abstract Task TimeoutsForDifferentTypesCanBeCreatedFetchedSuccessfully();
    protected async Task TimeoutsForDifferentTypesCanBeCreatedFetchedSuccessfully(Task<ITimeoutStore> storeTask)
    {
        var store = await storeTask;
        var flowId0 = TestStoredId.Create();
        var flowId1 = TestStoredId.Create();
        
        var timeoutId = "TimeoutId".ToEffectId();
        var expiry = DateTime.UtcNow.Date.AddDays(1).Ticks;
        await store.UpsertTimeout(new StoredTimeout(flowId0, timeoutId, expiry), overwrite: true);
        await store.UpsertTimeout(new StoredTimeout(flowId1, timeoutId, expiry), overwrite: true);
        
        var timeouts = await store.GetTimeouts(expiresBefore: expiry + 1).ToListAsync();
        
        timeouts.Count.ShouldBe(2);

        var timeout0 = timeouts.Single(t => t.StoredId == flowId0);
        timeout0.StoredId.ShouldBe(flowId0);
        timeout0.TimeoutId.ShouldBe(timeoutId);
        timeout0.Expiry.ShouldBe(expiry);
        
        var timeout1 = timeouts.Single(t => t.StoredId == flowId1);
        timeout1.StoredId.ShouldBe(flowId1);
        timeout1.TimeoutId.ShouldBe(timeoutId);
        timeout1.Expiry.ShouldBe(expiry);

        timeouts = await store.GetTimeouts(expiresBefore: expiry - 1).ToListAsync();
        timeouts.ShouldBeEmpty();
    }
    
    public abstract Task CancellingNonExistingTimeoutDoesResultInIO();
    protected async Task CancellingNonExistingTimeoutDoesResultInIO(Task<IFunctionStore> storeTask)
    {
        var removeCount = 0;
        var store = new TimeoutStoreDecorator((await storeTask).TimeoutStore, removeTimeoutCallback: () => removeCount++);
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        
        var registeredTimeouts = new RegisteredTimeouts(storedId, store, CreateEffect(flowId, storedId, await storeTask), () => DateTime.UtcNow);
        
        var pendingTimeouts = await registeredTimeouts.PendingTimeouts();
        pendingTimeouts.ShouldBeEmpty();

        await registeredTimeouts.RegisterTimeout("SomeOtherTimeoutId".ToEffectId(), expiresIn: TimeSpan.FromHours(1));
        
        await registeredTimeouts.CancelTimeout("SomeTimeoutId".ToEffectId());
        
        removeCount.ShouldBe(1);
    }
    
    private Effect CreateEffect(FlowId flowId, StoredId storedId, IFunctionStore functionStore)
    {
        var lazyExistingEffects = new Lazy<Task<IReadOnlyList<StoredEffect>>>(() => Task.FromResult((IReadOnlyList<StoredEffect>) new List<StoredEffect>()));
        var effectResults = new EffectResults(flowId, storedId, lazyExistingEffects, functionStore.EffectsStore, DefaultSerializer.Instance);
        var effect = new Effect(effectResults);
        return effect;
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

        public Task RemoveTimeout(StoredId storedId, EffectId timeoutId)
        {
            _removeTimeoutCallback?.Invoke();
            return _inner.RemoveTimeout(storedId, timeoutId);
        }
        
        public Task Remove(StoredId storedId)
            => _inner.Remove(storedId); 

        public Task<IEnumerable<StoredTimeout>> GetTimeouts(long expiresBefore)
            => _inner.GetTimeouts(expiresBefore);

        public Task<IEnumerable<StoredTimeout>> GetTimeouts(StoredId flowId)
            => _inner.GetTimeouts(flowId);
    }
}