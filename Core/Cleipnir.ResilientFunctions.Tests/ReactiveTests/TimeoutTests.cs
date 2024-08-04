using System;
using System.Collections.Generic;
using System.Linq;
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
        var timeoutId = "TimeoutId";
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var timeoutProviderStub = new TimeoutsStub();
        var source = new TestSource(timeoutProviderStub);

        var task = source.TakeUntilTimeout(timeoutId, expiresAt).First();
        
        await BusyWait.Until(() => timeoutProviderStub.Registrations.Any());
        
        var (id, expiry) = timeoutProviderStub.Registrations.Single();
        id.ShouldBe(timeoutId);
        expiry.ShouldBe(expiresAt);
        
        source.SignalNext(new TimeoutEvent(timeoutId, expiresAt), new InterruptCount(1));

        await BusyWait.Until(() => task.IsCompleted);
        task.IsCompleted.ShouldBeTrue();

        await Should.ThrowAsync<NoResultException>(task);
    }
    
    [TestMethod]
    public async Task StreamCompletesAndReturnsNothingAfterFiredTimeoutEvent()
    {
        var timeoutId = "TimeoutId";
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var timeoutProviderStub = new TimeoutsStub();
        var source = new TestSource(timeoutProviderStub);

        var task = source.TakeUntilTimeout(timeoutId, expiresAt).FirstOrNone();
        
        await BusyWait.Until(() => timeoutProviderStub.Registrations.Any());
        
        source.SignalNext(new TimeoutEvent(timeoutId, expiresAt), new InterruptCount(1));

        await BusyWait.Until(() => task.IsCompleted);
        task.IsCompletedSuccessfully.ShouldBeTrue();

        var option = await task;
        option.HasValue.ShouldBeFalse();
        
        var (id, expiry) = timeoutProviderStub.Registrations.Single();
        id.ShouldBe(timeoutId);
        expiry.ShouldBe(expiresAt);
    }
    
    [TestMethod]
    public async Task StreamCompletesSuccessfullyWhenEventSupersedesTimeout()
    {
        var timeoutId = "TimeoutId";
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var timeoutProviderStub = new TimeoutsStub();
        var source = new TestSource(timeoutProviderStub);

        var task = source.TakeUntilTimeout(timeoutId, expiresAt).First();
        
        source.SignalNext("Hello", new InterruptCount(1));

        await BusyWait.Until(() => task.IsCompleted);
        task.IsCompletedSuccessfully.ShouldBeTrue();
        task.Result.ShouldBe("Hello");
    }
    
    [TestMethod]
    public async Task StreamCompletesSuccessfullyWithValuedOptionWhenEventSupersedesTimeout()
    {
        var timeoutId = "TimeoutId";
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var timeoutProviderStub = new TimeoutsStub();
        var source = new TestSource(timeoutProviderStub);

        var task = source.TakeUntilTimeout(timeoutId, expiresAt).FirstOrNone();
        
        source.SignalNext("Hello", new InterruptCount(1));

        await BusyWait.Until(() => task.IsCompleted);
        task.IsCompletedSuccessfully.ShouldBeTrue();
        var option = task.Result;
        option.HasValue.ShouldBeTrue();
        option.Value.ShouldBe("Hello");
    }
    
    private class TimeoutsStub : ITimeouts
    {
        public List<Tuple<string, DateTime>> Registrations
        {
            get
            {
                lock (_sync)
                    return _registrations.ToList();
            }
        }

        private readonly object _sync = new();
        private readonly List<Tuple<string, DateTime>> _registrations = new();
            
        public Task RegisterTimeout(string timeoutId, DateTime expiresAt)
        {
            lock (_sync)
                _registrations.Add(Tuple.Create(timeoutId, expiresAt));

            return Task.CompletedTask;
        }

        public Task RegisterTimeout(string timeoutId, TimeSpan expiresIn)
            => Task.FromException(new Exception("Stub-method invocation"));

        public Task CancelTimeout(string timeoutId)
            => Task.FromException(new Exception("Stub-method invocation"));
        public Task<IReadOnlyList<TimeoutEvent>> PendingTimeouts()
            => Task.FromException<IReadOnlyList<TimeoutEvent>>(new Exception("Stub-method invocation"));
    }
}