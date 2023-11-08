using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Origin;
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
        
        var timeoutProviderStub = new TimeoutProviderStub();
        var source = new Source(timeoutProviderStub);

        var task = source.TakeUntilTimeout(timeoutId, expiresAt).First();
        
        source.SignalNext(new TimeoutEvent(timeoutId, expiresAt));

        task.IsCompleted.ShouldBeTrue();

        await Should.ThrowAsync<NoResultException>(task);
        
        await BusyWait.UntilAsync(() => timeoutProviderStub.Registrations.Any());
        
        var (id, expiry) = timeoutProviderStub.Registrations.Single();
        id.ShouldBe(timeoutId);
        expiry.ShouldBe(expiresAt);
    }
    
    [TestMethod]
    public async Task StreamCompletesAndReturnsNothingAfterFiredTimeoutEvent()
    {
        var timeoutId = "TimeoutId";
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var timeoutProviderStub = new TimeoutProviderStub();
        var source = new Source(timeoutProviderStub);

        var task = source.TakeUntilTimeout(timeoutId, expiresAt).FirstOrNone();
        
        source.SignalNext(new TimeoutEvent(timeoutId, expiresAt));

        task.IsCompletedSuccessfully.ShouldBeTrue();

        var option = await task;
        option.HasValue.ShouldBeFalse();
        
        await BusyWait.UntilAsync(() => timeoutProviderStub.Registrations.Any());
        
        var (id, expiry) = timeoutProviderStub.Registrations.Single();
        id.ShouldBe(timeoutId);
        expiry.ShouldBe(expiresAt);
    }
    
    [TestMethod]
    public async Task StreamEmitsErrorAfterFiredTimeoutEvent()
    {
        var timeoutId = "TimeoutId";
        var expiresAt = DateTime.UtcNow.Add(TimeSpan.FromMinutes(15));
        
        var timeoutProviderStub = new TimeoutProviderStub();
        var source = new Source(timeoutProviderStub);

        var task = source.FailOnTimeout(timeoutId, expiresAt).First();
        
        await BusyWait.UntilAsync(() => timeoutProviderStub.Registrations.Any());
        var (id, expiry) = timeoutProviderStub.Registrations.Single();
        id.ShouldBe(timeoutId);
        expiry.ShouldBe(expiresAt);
        
        source.SignalNext(new TimeoutEvent(timeoutId, expiresAt));

        task.IsCompleted.ShouldBeTrue();
        task.Status.ShouldBe(TaskStatus.Faulted);

        try
        {
            await task;
        }
        catch (TimeoutException timeoutException)
        {
            timeoutException.Message.Contains(timeoutId).ShouldBeTrue();
        }
    }

    private class TimeoutProviderStub : ITimeoutProvider
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
            
        public Task RegisterTimeout(string timeoutId, DateTime expiresAt, bool overwrite = false)
        {
            lock (_sync)
                _registrations.Add(Tuple.Create(timeoutId, expiresAt));

            return Task.CompletedTask;
        }

        public Task RegisterTimeout(string timeoutId, TimeSpan expiresIn, bool overwrite = false)
        {
            throw new NotImplementedException();
        }

        public Task CancelTimeout(string timeoutId)
        {
            throw new NotImplementedException();
        }

        public Task<List<TimeoutEvent>> PendingTimeouts()
        {
            throw new NotImplementedException();
        }
    }
}