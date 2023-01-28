using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
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
        var source = new Source(NoOpTimeoutProvider.Instance);
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
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext(1);
            
        var next = source.Next();
        source.SignalNext(2);
            
        next.IsCompletedSuccessfully.ShouldBeTrue();
        next.Result.ShouldBe(1);
            
        source.SignalNext(3); //should not thrown an error
    }
    
    [TestMethod]
    public void NextOperatorWithSuspensionAndTimeoutSucceedsWithImmediateSignal()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        var nextOrSuspend = source.OfType<int>().SuspendUntilNext(TimeSpan.FromMilliseconds(250));
        source.SignalNext(1);
        nextOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
        
        nextOrSuspend.Result.ShouldBe(1);
    }
    
    [TestMethod]
    public async Task NextOperatorWithSuspensionAndTimeoutThrowsExceptionWhenNothingIsSignaled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        await Should.ThrowAsync<SuspendInvocationException>(
            () => source.OfType<int>().SuspendUntilNext(TimeSpan.FromMilliseconds(10))
        );
    }

    [TestMethod]
    public void ThrownExceptionInOperatorResultsInLeafThrowingSameException()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var next = source.Where(_ => throw new InvalidOperationException("oh no")).Next();
            
        next.IsCompleted.ShouldBeFalse();
        source.SignalNext("hello");
            
        next.IsFaulted.ShouldBeTrue();
        next.Exception!.InnerException.ShouldBeOfType<InvalidOperationException>();
    }
        
    [TestMethod]
    public void SubscriptionWithSkip1CompletesAfterNonSkippedSubscription()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
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
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        source.SignalNext("world");

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
        var source = new Source(NoOpTimeoutProvider.Instance);

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
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        source.SignalNext("world");

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
            
        subscription.DeliverExisting();
            
        completed.ShouldBeTrue();
        failed.ShouldBeFalse();
        latest.ShouldBe("hello");
    }
        
    [TestMethod]
    public void StreamCanBeReplayedToCertainEventCountWhenFailedEarlySuccessfully()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        source.SignalNext("world");

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
            
        subscription.DeliverExisting();
            
        completed.ShouldBeFalse();
        failed.ShouldBeTrue();
        latest.ShouldBe("");
    }
    
    [TestMethod]
    public void MergeTests()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        var toUpper = source.OfType<string>().Select(s => s.ToUpper());

        var emitsTask = source.Merge(toUpper).Take(2).ToList();
        
        source.SignalNext("hello");
        
        emitsTask.IsCompletedSuccessfully.ShouldBeTrue();
        var emits = emitsTask.Result;
        emits.Count.ShouldBe(2);
        emits[0].ShouldBe("hello");
        emits[1].ShouldBe("HELLO");
    }
    
    [TestMethod]
    public void OfTwoTypesTest()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        source.SignalNext(2);
        
        {
            var either = source.OfTypes<string, int>().Next().Result;
            either.ValueSpecified.ShouldBe(Either<string, int>.Value.First);
            either.HasFirst.ShouldBeTrue();
            either.Do(first: s => s.ShouldBe("hello"), second: _ => throw new Exception("Unexpected value"));
            var matched = either.Match(first: s => s.ToUpper(), second: _ => throw new Exception("Unexpected value"));
            matched.ShouldBe("HELLO");
        }

        {
            var either = source.Skip(1).OfTypes<string, int>().Next().Result;
            either.ValueSpecified.ShouldBe(Either<string, int>.Value.Second);
            either.HasFirst.ShouldBeFalse();
            either.Do(first: _ => throw new Exception("Unexpected value"), second: i => i.ShouldBe(2));
            var matched = either.Match(first: _ => throw new Exception("Unexpected value"), second: i => i.ToString());
            matched.ShouldBe("2");
        }
    }
    
    [TestMethod]
    public void OfThreeTypesTest()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        source.SignalNext(2);
        source.SignalNext(25L);
        
        {
            var either = source.OfTypes<string, int, long>().Next().Result;
            either.ValueSpecified.ShouldBe(Either<string, int, long>.Value.First);
            either.HasFirst.ShouldBeTrue();
            either.Do(
                first: s => s.ShouldBe("hello"),
                second: _ => throw new Exception("Unexpected value"),
                third: _ => throw new Exception("Unexpected value")
            );
            var matched = either.Match(
                first: s => s.ToUpper(), 
                second: _ => throw new Exception("Unexpected value"),
                third: _ => throw new Exception("Unexpected value"));
            matched.ShouldBe("HELLO");
        }

        {
            var either = source.Skip(2).OfTypes<string, int, long>().Next().Result;
            either.ValueSpecified.ShouldBe(Either<string, int, long>.Value.Third);
            either.HasFirst.ShouldBeFalse();
            either.Do(
                first: _ => throw new Exception("Unexpected value"),
                second: _ => throw new Exception("Unexpected value"),
                third: i => i.ShouldBe(25L)
            );
            var matched = either.Match(
                first: _ => throw new Exception("Unexpected value"),
                second: _ => throw new Exception("Unexpected value"),
                third: i => i.ToString()
            );
            matched.ShouldBe("25");
        }
    }

    [TestMethod]
    public async Task AsyncEnumerableSunshineTest()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var emits = new SyncedList<string>();
        source.SignalNext("hello");

        async Task AwaitForeach()
        {
            await foreach (var item in source.OfType<string>())
                emits.Add(item);
        }

        var task = AwaitForeach();
        await BusyWait.UntilAsync(() => emits.Count == 1);
        emits.Count.ShouldBe(1);
        emits[0].ShouldBe("hello");
        task.IsCompleted.ShouldBeFalse();

        source.SignalNext("world");
        await BusyWait.UntilAsync(() => emits.Count == 2);
        emits.Count.ShouldBe(2);
        emits[1].ShouldBe("world");
        task.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        await BusyWait.UntilAsync(() => task.IsCompletedSuccessfully);
        emits.Count.ShouldBe(2);

        await task;
    }
    
    [TestMethod]
    public async Task AsyncEnumerableThrownExceptionTest()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var emits = new SyncedList<string>();
        source.SignalNext("hello");

        async Task AwaitForeach()
        {
            await foreach (var item in source.OfType<string>())
                emits.Add(item);
        }

        var task = AwaitForeach();
        await BusyWait.UntilAsync(() => emits.Count == 1);
        emits.Count.ShouldBe(1);
        emits[0].ShouldBe("hello");
        task.IsCompleted.ShouldBeFalse();

        source.SignalError(new TimeoutException());
        await BusyWait.UntilAsync(() => task.IsFaulted);
        await Should.ThrowAsync<TimeoutException>(task);
    }

    [TestMethod]
    public async Task BufferOperatorTest()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");

        var nextTask = source.Buffer(2).Next();
        var listTask = source.Buffer(2).ToList();
        
        nextTask.IsCompleted.ShouldBeFalse();
        listTask.IsCompleted.ShouldBeFalse();
        source.SignalNext("world");
        
        nextTask.IsCompletedSuccessfully.ShouldBeTrue();
        var result = await nextTask;
        result.Count.ShouldBe(2);
        result[0].ShouldBe("hello");
        result[1].ShouldBe("world");

        source.SignalNext("hello");
        source.SignalNext("universe");
        source.SignalCompletion();
        
        listTask.IsCompletedSuccessfully.ShouldBeTrue();
        var list = await listTask;
        list.Count.ShouldBe(2);
        var flatten = list.SelectMany(_ => _).ToList();
        flatten.Count.ShouldBe(4);
        flatten[0].ShouldBe("hello");
        flatten[1].ShouldBe("world");
        flatten[2].ShouldBe("hello");
        flatten[3].ShouldBe("universe");
    }
    
    [TestMethod]
    public async Task BufferOperatorOnCompletionEmitsBufferContent()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");

        var nextTask = source.Buffer(2).Next();
        
        source.SignalCompletion();
        nextTask.IsCompletedSuccessfully.ShouldBeTrue();
        var emitted = await nextTask;
        emitted.Count.ShouldBe(1);
        emitted[0].ShouldBe("hello");
    }
}
