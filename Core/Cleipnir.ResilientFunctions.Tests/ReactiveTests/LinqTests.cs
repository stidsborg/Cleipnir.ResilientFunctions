using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class LinqTests
{
    [TestMethod]
    public void EventsCanBeFilteredByType()
    {
        var source = new Source<object>();
        var nextStringEmitted = source.OfType<string>().Next();
        nextStringEmitted.IsCompleted.ShouldBeFalse();
            
        source.SignalNext(1);
        nextStringEmitted.IsCompleted.ShouldBeFalse();

        source.SignalNext("hello");

        nextStringEmitted.IsCompleted.ShouldBeTrue();
        nextStringEmitted.Result.ShouldBe("hello");
    }
        
    [TestMethod]
    public void NextOperatorEmitsLastEmittedEventAfterCompletionOfTheStream()
    {
        var source = new Source<int>();
        source.SignalNext(1);
            
        var next = source.Next();
        source.SignalNext(2);
            
        next.IsCompletedSuccessfully.ShouldBeTrue();
        next.Result.ShouldBe(1);
            
        source.SignalNext(3); //should not thrown an error
    }

    [TestMethod]
    public void ThrownExceptionInOperatorResultsInLeafThrowingSameException()
    {
        var source = new Source<string>();
        var next = source.Where(_ => throw new InvalidOperationException("oh no")).Next();
            
        next.IsCompleted.ShouldBeFalse();
        source.SignalNext("hello");
            
        next.IsFaulted.ShouldBeTrue();
        next.Exception!.InnerException.ShouldBeOfType<InvalidOperationException>();
    }
        
    [TestMethod]
    public void SubscriptionWithSkip1CompletesAfterNonSkippedSubscription()
    {
        var source = new Source<string>();
        var next1 = source.Next();
        var next2 = source.Skip(1).Next();
            
        source.SignalNext("hello");
        next1.IsCompletedSuccessfully.ShouldBeTrue();
        next2.IsCompleted.ShouldBeFalse();
        source.SignalNext("world");
        next2.IsCompletedSuccessfully.ShouldBeTrue();
    }
        
    [TestMethod]
    public async Task EventsAreEmittedBreathFirstWhenStreamsAreInSameSubscriptionGroup()
    {
        var subscription2OnNextFlag = new SyncedFlag();
        var completeSubscription2OnNextFlag = new SyncedFlag();
        var source = new Source<string>();
        source.SignalNext("hello");
        source.SignalNext("world");

        var completed1 = false;
        var failed1 = false;
        var latest1 = "";
        var subscription1 = source.Subscribe(
            onNext: s => latest1 = s,
            onCompletion: () => completed1 = true,
            onError: _ => failed1 = true
        );
            
        var completed2 = false;
        var failed2 = false;
        var latest2 = "";
        var subscription2 = source.Subscribe(
            onNext: s =>
            {
                latest2 = s;
                subscription2OnNextFlag.Raise();
                completeSubscription2OnNextFlag.WaitForRaised().Wait();
            },
            onCompletion: () => completed1 = true,
            onError: _ => failed1 = true,
            subscriptionGroupId: subscription1.SubscriptionGroupId
        );
            
        var deliverExistingTask = Task.Run(() => subscription1.DeliverExisting());
        await subscription2OnNextFlag.WaitForRaised();
            
        latest1.ShouldBe("hello");
        latest2.ShouldBe("hello");
        completed1.ShouldBeFalse();
        completed2.ShouldBeFalse();

        deliverExistingTask.IsCompleted.ShouldBeFalse();
        completeSubscription2OnNextFlag.Raise();
        var deliveredExistingCount = await deliverExistingTask;
        deliveredExistingCount.ShouldBe(2);
        latest1.ShouldBe("world");
        latest2.ShouldBe("world");
            
        failed1.ShouldBeFalse();
        failed2.ShouldBeFalse();
    }
        
    [TestMethod]
    public void StreamsInSameSubscriptionGroupCanBeDisposedSuccessfully()
    {
        var source = new Source<string>();

        var subscription1Emits = 0;
        var subscription2Emits = 0;
        var subscription1 = source.Subscribe(
            onNext: _ => subscription1Emits++, 
            onCompletion: () => {}, 
            onError: _ => {}
        );
            
        var subscription2 = source.Subscribe(
            onNext: _ => subscription2Emits++, 
            onCompletion: () => {}, 
            onError: _ => {},
            subscription1.SubscriptionGroupId
        );
            
        subscription1.DeliverExistingAndFuture();
            
        source.SignalNext("hello");
        subscription1Emits.ShouldBe(1);
        subscription2Emits.ShouldBe(1);
            
        subscription1.Dispose();
            
        source.SignalNext("world");
        subscription1Emits.ShouldBe(1);
        subscription2Emits.ShouldBe(2);
    }
        
    [TestMethod]
    public void StreamCanBeReplayedToCertainEventCountWhenCompletedEarlySuccessfully()
    {
        var source = new Source<string>();
        source.SignalNext("hello");
        source.SignalNext("world");

        var completed = false;
        var failed = false;
        var latest = "";
        var subscription = source.Take(1).Subscribe(
            onNext: s => latest = s,
            onCompletion: () => completed = true,
            onError: _ => failed = true
        );

        completed.ShouldBeFalse();
        failed.ShouldBeFalse();
        latest.ShouldBe("");
            
        subscription.DeliverExisting();
            
        completed.ShouldBeTrue();
        failed.ShouldBeFalse();
        latest.ShouldBe("hello");
    }
        
    [TestMethod]
    public void StreamCanBeReplayedToCertainEventCountWhenFailedEarlySuccessfully()
    {
        var source = new Source<string>();
        source.SignalNext("hello");
        source.SignalNext("world");

        var completed = false;
        var failed = false;
        var latest = "";
        var subscription = source
            .Select<string, string>(_ => throw new Exception("oh no"))
            .Subscribe(
                onNext: s => latest = s,
                onCompletion: () => completed = true,
                onError: _ => failed = true
            );

        completed.ShouldBeFalse();
        failed.ShouldBeFalse();
        latest.ShouldBe("");
            
        subscription.DeliverExisting();
            
        completed.ShouldBeFalse();
        failed.ShouldBeTrue();
        latest.ShouldBe("");
    }
    
    [TestMethod]
    public void MergeTests()
    {
        var source = new Source<string>();

        var toUpper = source.Select(s => s.ToUpper());

        var emitsTask = source.Merge(toUpper).Take(2).ToList();
        
        source.SignalNext("hello");
        
        emitsTask.IsCompletedSuccessfully.ShouldBeTrue();
        var emits = emitsTask.Result;
        emits.Count.ShouldBe(2);
        emits[0].ShouldBe("hello");
        emits[1].ShouldBe("HELLO");
    }
}