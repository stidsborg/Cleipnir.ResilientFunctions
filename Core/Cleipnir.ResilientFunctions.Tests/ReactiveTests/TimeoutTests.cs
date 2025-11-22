using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class TimeoutTests
{
    [TestMethod]
    public async Task StreamCompletesAndThrowsNoResultExceptionAfterFiredTimeoutEvent()
    {
        var timeoutId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var registeredTimeoutsStub = new RegisteredTimeoutsStub();
        var source = new TestSource(registeredTimeoutsStub);

        var task = source.TakeUntilTimeout(timeoutId, expiresAt).First();
        
        await BusyWait.Until(() => registeredTimeoutsStub.Registrations.Any());
        
        var (id, expiry) = registeredTimeoutsStub.Registrations.Single();
        id.ShouldBe(timeoutId);
        expiry.ShouldBe(expiresAt);
        
        source.SignalNext(new TimeoutEvent(timeoutId, expiresAt));

        await BusyWait.Until(() => task.IsCompleted);
        task.IsCompleted.ShouldBeTrue();

        await Should.ThrowAsync<NoResultException>(task);
    }
    
    [TestMethod]
    public async Task StreamCompletesAndReturnsNothingAfterFiredTimeoutEvent()
    {
        var timeoutId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var registeredTimeoutsStub = new RegisteredTimeoutsStub();
        var source = new TestSource(registeredTimeoutsStub);

        var task = source.TakeUntilTimeout(timeoutId, expiresAt).FirstOrNone();
        
        await BusyWait.Until(() => registeredTimeoutsStub.Registrations.Any());
        
        source.SignalNext(new TimeoutEvent(timeoutId, expiresAt));

        await BusyWait.Until(() => task.IsCompleted);
        task.IsCompletedSuccessfully.ShouldBeTrue();

        var option = await task;
        option.HasValue.ShouldBeFalse();
        
        var (id, expiry) = registeredTimeoutsStub.Registrations.Single();
        id.ShouldBe(timeoutId);
        expiry.ShouldBe(expiresAt);
    }
    
    [TestMethod]
    public async Task StreamCompletesSuccessfullyWhenEventSupersedesTimeout()
    {
        var timeoutId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var registeredTimeoutsStub = new RegisteredTimeoutsStub();
        var source = new TestSource(registeredTimeoutsStub);

        var task = source.TakeUntilTimeout(timeoutId, expiresAt).First();
        
        source.SignalNext("Hello");

        await BusyWait.Until(() => task.IsCompleted);
        task.IsCompletedSuccessfully.ShouldBeTrue();
        task.Result.ShouldBe("Hello");
    }
    
    [TestMethod]
    public async Task StreamCompletesSuccessfullyWithValuedOptionWhenEventSupersedesTimeout()
    {
        var timeoutId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var registeredTimeoutsStub = new RegisteredTimeoutsStub();
        var source = new TestSource(registeredTimeoutsStub);

        var task = source.TakeUntilTimeout(timeoutId, expiresAt).FirstOrNone();
        
        source.SignalNext("Hello");

        await BusyWait.Until(() => task.IsCompleted);
        task.IsCompletedSuccessfully.ShouldBeTrue();
        var option = task.Result;
        option.HasValue.ShouldBeTrue();
        option.Value.ShouldBe("Hello");
        
        registeredTimeoutsStub.Cancelled.ShouldBe(timeoutId);
    }
    
    [TestMethod]
    public async Task ExistingTimeoutEventInMessagesAvoidRegisteredTimeoutsCancellation()
    {
        var timeoutId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var registeredTimeoutsStub = new RegisteredTimeoutsStub();
        var source = new TestSource(registeredTimeoutsStub);
        source.SignalNext(new TimeoutEvent(timeoutId, expiresAt));

        var task = await source.TakeUntilTimeout(timeoutId, expiresAt).FirstOrNone();
        task.HasValue.ShouldBeFalse();
    }
    
    private class RegisteredTimeoutsStub : IRegisteredTimeouts
    {
        public List<Tuple<EffectId, DateTime>> Registrations
        {
            get
            {
                lock (_sync)
                    return _registrations
                        .Select(kv => Tuple.Create(kv.Key, kv.Value))
                        .ToList();
            }
        }

        public volatile EffectId? Cancelled = null; 

        private readonly Lock _sync = new();
        private readonly Dictionary<EffectId, DateTime> _registrations = new();
            
        public Task<Tuple<TimeoutStatus, DateTime>> RegisterTimeout(EffectId timeoutId, DateTime expiresAt, bool publishMessage)
        {
            lock (_sync)
                _registrations[timeoutId] = expiresAt;

            return Tuple.Create(TimeoutStatus.Registered, expiresAt).ToTask();
        }

        public Task<Tuple<TimeoutStatus, DateTime>> RegisterTimeout(EffectId timeoutId, TimeSpan expiresIn, bool publishMessage)
            => RegisterTimeout(timeoutId, DateTime.UtcNow.Add(expiresIn), publishMessage);

        public Task CancelTimeout(EffectId timeoutId)
        {
            Cancelled = timeoutId;
            return Task.CompletedTask;
        }

        public Task CompleteTimeout(EffectId timeoutId) => Task.CompletedTask;

        public Task<IReadOnlyList<RegisteredTimeout>> PendingTimeouts()
            => Task.FromException<IReadOnlyList<RegisteredTimeout>>(new Exception("Stub-method invocation"));

        public void Dispose() { }
    }
}