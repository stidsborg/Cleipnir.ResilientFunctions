using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Origin;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class ReactiveChainSemanticsTests
{
    [TestMethod]
    public async Task EventsAreEmittedBreathFirstWhenStreamsAreInSameSubscriptionGroup()
    {
        var subscription2OnNextFlag = new SyncedFlag();
        var completeSubscription2OnNextFlag = new SyncedFlag();
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello", new InterruptCount(1));
        source.SignalNext("world", new InterruptCount(2));

        var completed1 = false;
        var failed1 = false;
        var latest1 = "";
        var subscription1 = source.OfType<string>().Subscribe(
            onNext: s => latest1 = s,
            onCompletion: () => completed1 = true,
            onError: _ => failed1 = true
        );
            
        var completed2 = false;
        var failed2 = false;
        var latest2 = "";
        var subscription2 = source.OfType<string>().Subscribe(
            onNext: s =>
            {
                latest2 = s;
                subscription2OnNextFlag.Raise();
                completeSubscription2OnNextFlag.WaitForRaised().Wait();
            },
            onCompletion: () => completed1 = true,
            onError: _ => failed1 = true,
            addToSubscriptionGroup: subscription1.Group
        );
            
        var deliverExistingTask = Task.Run(() =>
        {
            subscription1.SyncStore(TimeSpan.Zero);
            subscription1.PushMessages();
        });
        await subscription2OnNextFlag.WaitForRaised();
            
        latest1.ShouldBe("hello");
        latest2.ShouldBe("hello");
        completed1.ShouldBeFalse();
        completed2.ShouldBeFalse();

        deliverExistingTask.IsCompleted.ShouldBeFalse();
        completeSubscription2OnNextFlag.Raise();
        await deliverExistingTask;
        
        latest1.ShouldBe("world");
        latest2.ShouldBe("world");
            
        failed1.ShouldBeFalse();
        failed2.ShouldBeFalse();
    }
        
    [TestMethod]
    public void StreamCanBeReplayedToCertainEventCountWhenCompletedEarlySuccessfully()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello", new InterruptCount(1));
        source.SignalNext("world", new InterruptCount(2));

        var completed = false;
        var failed = false;
        var latest = "";
        var subscription = source.OfType<string>().Take(1).Subscribe(
            onNext: s => latest = s,
            onCompletion: () => completed = true,
            onError: _ => failed = true
        );

        completed.ShouldBeFalse();
        failed.ShouldBeFalse();
        latest.ShouldBe("");

        subscription.PushMessages();
            
        completed.ShouldBeTrue();
        failed.ShouldBeFalse();
        latest.ShouldBe("hello");
    }
        
    [TestMethod]
    public void StreamCanBeReplayedToCertainEventCountWhenFailedEarlySuccessfully()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello", new InterruptCount(1));
        source.SignalNext("world", new InterruptCount(2));

        var completed = false;
        var failed = false;
        var latest = "";
        var subscription = source
            .OfType<string>()
            .Select<string, string>(_ => throw new Exception("oh no"))
            .Subscribe(
                onNext: s => latest = s,
                onCompletion: () => completed = true,
                onError: _ => failed = true
            );

        completed.ShouldBeFalse();
        failed.ShouldBeFalse();
        latest.ShouldBe("");

        subscription.PushMessages();
            
        completed.ShouldBeFalse();
        failed.ShouldBeTrue();
        latest.ShouldBe("");
    }
    
    [TestMethod]
    public void ExistingPropertyContainsPreviouslyEmittedEvents()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello", new InterruptCount(1));
        var existing = source.Existing.ToList();
        existing.Count.ShouldBe(1);
        existing[0].ShouldBe("hello");

        source.SignalNext("world", new InterruptCount(2)); 
        
        existing = source.Existing.ToList();
        existing.Count.ShouldBe(2);
        existing[0].ShouldBe("hello");
        existing[1].ShouldBe("world");
    }

    [TestMethod]
    public void StreamUnsubscribesToFutureEventsAfterDeliverExistingInvocation()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var emitted = 0;

        source.SignalNext("hello", new InterruptCount(1));
        
        var subscription = source
            .Subscribe(
                onNext: _ => emitted++,
                onError: _ => { },
                onCompletion: () => { }
            );

        subscription.PushMessages();
        
        emitted.ShouldBe(1);

        source.SignalNext("world", new InterruptCount(1));
        
        emitted.ShouldBe(1);
    }
}