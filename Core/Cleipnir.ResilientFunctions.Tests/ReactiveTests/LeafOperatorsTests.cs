using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Origin;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class LeafOperatorsTests
{
    [TestMethod]
    public void SourceCanBeSubscribedToMultipleTimes()
    {
        var source = new TestSource();
        source.SignalNext("hello");
        var emits = new List<object>();
        var completed = false;
        var error = default(Exception);
        var subscription = source.Subscribe(
            onNext: e => emits.Add(e),
            onCompletion: () => completed = true,
            onError: e => error = e
        );

        subscription.PushMessages();
        emits.Count.ShouldBe(1);
        emits[0].ShouldBe("hello");
        completed.ShouldBeFalse();
        error.ShouldBeNull();
        subscription.PushMessages();
        emits.Count.ShouldBe(1);
        emits[0].ShouldBe("hello");
        completed.ShouldBeFalse();
        error.ShouldBeNull();
    }

    #region First(s)

    [TestMethod]
    public void FirstOperatorEmitsFirstEmittedEvent()
    {
        var source = new TestSource();
        source.SignalNext(1);
            
        var next = source.First();
        source.SignalNext(2);
            
        next.IsCompletedSuccessfully.ShouldBeTrue();
        next.Result.ShouldBe(1);
            
        source.SignalNext(3); //should not thrown an error
    }
    
    [TestMethod]
    public void FirstOperatorWithSuspensionEmitsFirstValue()
    {
        var source = new TestSource();

        source.SignalNext(1);
        source.SignalNext(2);
        var nextOrSuspend = source.OfType<int>().First(TimeSpan.Zero);
        
        nextOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
        nextOrSuspend.Result.ShouldBe(1);
    }
    
    [TestMethod]
    public async Task FirstOperatorWithSuspensionAndTimeoutSucceedsOnImmediateSignal()
    {
        var source = new TestSource();

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        var nextOrSuspend = source.First(maxWait: TimeSpan.FromSeconds(2));
        source.SignalNext(1);
        source.SignalNext(2);

        await nextOrSuspend;
        stopWatch.Stop();
        
        nextOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
        nextOrSuspend.Result.ShouldBe(1);
        
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(2));
    }

    [TestMethod]
    public async Task FirstOperatorWithSuspensionAndTimeoutThrowsTimeoutExceptionWhenNothingIsSignalled()
    {
        var source = new TestSource();
        
        var nextOrSuspend = source.First(maxWait: TimeSpan.FromMilliseconds(100));
        
        await Should.ThrowAsync<SuspendInvocationException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task FirstOperatorWithSuspensionThrowsSuspensionExceptionWhenNothingIsSignaled()
    {
        var source = new TestSource();

        await Should.ThrowAsync<SuspendInvocationException>(
            () => source.OfType<int>().First(maxWait: TimeSpan.Zero)
        );
    }
    
    [TestMethod]
    public async Task FirstOperatorWithSuspensionAndTimeoutEventThrowsSuspensionExceptionWhenNothingIsSignalled()
    {
        var source = new TestSource();
        var timeoutEventId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.AddDays(1);

        source.SignalNext(new TimeoutEvent(2.ToEffectId(), expiresAt));
        
        var nextOrSuspend = source
            .TakeUntilTimeout(timeoutEventId, expiresAt)
            .OfType<string>()
            .First(maxWait: TimeSpan.Zero);
        
        await Should.ThrowAsync<SuspendInvocationException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task FirstOperatorWithSuspensionAndTimeoutEventReturnTimeoutOptionWithoutValueWhenTimeoutEventIsSignalled()
    {
        var source = new TestSource();
        var timeoutEventId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.AddDays(1);

        source.SignalNext(new TimeoutEvent(1.ToEffectId(), expiresAt));
        
        var nextOrSuspend = await source
            .TakeUntilTimeout(timeoutEventId, expiresAt)
            .OfType<string>()
            .FirstOrNone(TimeSpan.Zero);
        
        nextOrSuspend.HasValue.ShouldBeFalse();
    }
    
    [TestMethod]
    public async Task FirstOperatorWithSuspensionAndTimeoutEventReturnsValueOnSignal()
    {
        var source = new TestSource();
        var timeoutEventId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.AddDays(1);

        source.SignalNext(new TimeoutEvent(2.ToEffectId(), expiresAt));
        source.SignalNext("hallo");
        source.SignalNext("world");
        
        var nextOrSuspend = await source
            .TakeUntilTimeout(timeoutEventId, expiresAt)
            .OfType<string>()
            .FirstOrNone(TimeSpan.Zero);
        
        nextOrSuspend.HasValue.ShouldBeTrue();
        nextOrSuspend.Value.ShouldBe("hallo");
    }
    
    [TestMethod]
    public async Task FirstOfTypeReturnsValueOfTypeOnSignal()
    {
        var source = new TestSource();
        
        source.SignalNext("hallo");
        source.SignalNext("world");

        var firstOfType = source.FirstOfType<int>();
        firstOfType.IsCompleted.ShouldBeFalse();
        
        source.SignalNext(2);
        await BusyWait.Until(() => firstOfType.IsCompletedSuccessfully);
        firstOfType.Result.ShouldBe(2);
    }
    
    [TestMethod]
    public async Task FirstOfReturnsValueOfTypeOnSignal()
    {
        var source = new TestSource();
        
        source.SignalNext("hallo");
        source.SignalNext("world");

        var firstOfType = source.FirstOf<int>();
        firstOfType.IsCompleted.ShouldBeFalse();
        
        source.SignalNext(2);
        
        await BusyWait.Until(() => firstOfType.IsCompletedSuccessfully);
        firstOfType.Result.ShouldBe(2);
    }
    
    [TestMethod]
    public async Task EitherOrNoneOfTwoReturnsNoneOnCompletedStreamWithoutEmits()
    {
        var source = new TestSource();
        
        source.SignalNext("stop");

        var eitherOrNone = await source
            .TakeUntil(msg => msg is "stop")
            .OfTypes<int, string>()
            .FirstOrNone();
        
        eitherOrNone.HasNone.ShouldBeTrue();
        eitherOrNone.HasFirst.ShouldBeFalse();
        eitherOrNone.HasSecond.ShouldBeFalse();
    }
    
    [TestMethod]
    public async Task EitherOrNoneOfTwoReturnsFirstOnCompletedStreamWithEmit()
    {
        var source = new TestSource();
        
        source.SignalNext(1);

        var eitherOrNone = await source
            .OfTypes<int, string>()
            .FirstOrNone();
        
        eitherOrNone.HasNone.ShouldBeFalse();
        eitherOrNone.HasFirst.ShouldBeTrue();
        eitherOrNone.First.ShouldBe(1);
        eitherOrNone.HasSecond.ShouldBeFalse();
    }
    
    [TestMethod]
    public async Task EitherOrNoneOfTwoReturnsSecondOnCompletedStreamWithEmit()
    {
        var source = new TestSource();
        
        source.SignalNext("1");

        var eitherOrNone = await source
            .OfTypes<int, string>()
            .FirstOrNone();
        
        eitherOrNone.HasNone.ShouldBeFalse();
        eitherOrNone.HasFirst.ShouldBeFalse();
        eitherOrNone.HasSecond.ShouldBeTrue();
        eitherOrNone.Second.ShouldBe("1");
    }
    
    [TestMethod]
    public async Task EitherOrNoneOfThreeReturnsNoneOnCompletedStreamWithoutEmits()
    {
        var source = new TestSource();
        
        source.SignalNext("stop");

        var eitherOrNone = await source
            .TakeUntil(msg => msg is "stop")
            .OfTypes<int, string, long>()
            .FirstOrNone();
        
        eitherOrNone.HasNone.ShouldBeTrue();
        eitherOrNone.HasFirst.ShouldBeFalse();
        eitherOrNone.HasSecond.ShouldBeFalse();
        eitherOrNone.HasThird.ShouldBeFalse();
    }
    
    [TestMethod]
    public async Task EitherOrNoneOfThreeReturnsFirstOnCompletedStreamWithEmit()
    {
        var source = new TestSource();
        
        source.SignalNext(1);

        var eitherOrNone = await source
            .OfTypes<int, string, long>()
            .FirstOrNone();
        
        eitherOrNone.HasNone.ShouldBeFalse();
        eitherOrNone.HasFirst.ShouldBeTrue();
        eitherOrNone.First.ShouldBe(1);
        eitherOrNone.HasSecond.ShouldBeFalse();
        eitherOrNone.HasThird.ShouldBeFalse();
    }
    
    [TestMethod]
    public async Task EitherOrNoneOfThreeReturnsSecondOnCompletedStreamWithEmit()
    {
        var source = new TestSource();
        
        source.SignalNext("1");

        var eitherOrNone = await source
            .OfTypes<int, string, long>()
            .FirstOrNone();
        
        eitherOrNone.HasNone.ShouldBeFalse();
        eitherOrNone.HasFirst.ShouldBeFalse();
        eitherOrNone.HasSecond.ShouldBeTrue();
        eitherOrNone.Second.ShouldBe("1");
        eitherOrNone.HasThird.ShouldBeFalse();
    }
    
    [TestMethod]
    public async Task EitherOrNoneOfThreeReturnsThirdOnCompletedStreamWithEmit()
    {
        var source = new TestSource();
        
        source.SignalNext(1L);

        var eitherOrNone = await source
            .OfTypes<int, string, long>()
            .FirstOrNone();
        
        eitherOrNone.HasNone.ShouldBeFalse();
        eitherOrNone.HasFirst.ShouldBeFalse();
        eitherOrNone.HasSecond.ShouldBeFalse();
        eitherOrNone.HasThird.ShouldBeTrue();
        eitherOrNone.Third.ShouldBe(1L);
    }
    
    #endregion

    #region Last(s)

    [TestMethod]
    public async Task LastOperatorEmitsLastEmittedEventAfterStreamCompletion()
    {
        var source = new TestSource();
        source.SignalNext(1);
            
        var last = source.Last();
        source.SignalNext(2);
            
        last.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();

        await BusyWait.Until(() => last.IsCompletedSuccessfully);
        last.Result.ShouldBe(2);
    }
    
    [TestMethod]
    public void LastOperatorWithSuspensionEmitsLastValue()
    {
        var source = new TestSource();

        source.SignalNext(1);
        source.SignalNext(2);
        var lastOrSuspend = source.OfType<int>().Take(2).Last();
        
        lastOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
        lastOrSuspend.Result.ShouldBe(2);
    }

    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutSucceedsWithImmediateSignal()
    {
        var source = new TestSource();

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        var nextOrSuspend = source.Last(maxWait: TimeSpan.FromSeconds(1));
        source.SignalNext(1);
        source.SignalNext(2);

        source.SignalCompletion();
        
        await nextOrSuspend;
        stopWatch.Stop();
        
        nextOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
        nextOrSuspend.Result.ShouldBe(2);
        
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(1));
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutThrowsTimeoutExceptionWhenNothingIsSignalled()
    {
        var source = new TestSource();
        
        var nextOrSuspend = source.Last(maxWait: TimeSpan.FromMilliseconds(100));
        
        await Should.ThrowAsync<SuspendInvocationException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutThrowsNoResultExceptionWhenNothingIsSignalledAndStreamCompletes()
    {
        var source = new TestSource();
        
        source.SignalNext("hello");

        var nextOrSuspend = source
            .TakeUntilTimeout(1.ToEffectId(), expiresAt: DateTime.UtcNow)
            .Take(1)
            .OfType<int>()
            .Last(maxWait: TimeSpan.Zero);
        
        await Should.ThrowAsync<NoResultException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutEventThrowsSuspensionExceptionWhenNothingIsSignalled()
    {
        var source = new TestSource();
        var timeoutEventId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.AddDays(1);

        source.SignalNext(new TimeoutEvent(2.ToEffectId(), expiresAt));
        
        var nextOrSuspend = source
            .TakeUntilTimeout(timeoutEventId, expiresAt)
            .Last(maxWait: TimeSpan.Zero);
        
        await Should.ThrowAsync<SuspendInvocationException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task LastsOperatorReturnsAllEmitsBeforeCompletion()
    {
        var source = new TestSource();
        var lastsTask = source.Take(3).ToList();
        
        source.SignalNext(1);
        lastsTask.IsCompleted.ShouldBeFalse();
        source.SignalNext(2);
        lastsTask.IsCompleted.ShouldBeFalse();
        source.SignalNext(3);
        await BusyWait.Until(() => lastsTask.IsCompletedSuccessfully);

        source.SignalNext(4);
        
        await BusyWait.Until(() => lastsTask.IsCompletedSuccessfully);

        var emits = lastsTask.Result;
        emits.Count.ShouldBe(3);
        emits[0].ShouldBe(1);
        emits[1].ShouldBe(2);
        emits[2].ShouldBe(3);
    }
    
    [TestMethod]
    public void LastsWithCountOperatorReturnsAllEmitsAtReachedCount()
    {
        var source = new TestSource();
        var lastsTask = source.Lasts(count: 3);
        
        source.SignalNext(1);
        lastsTask.IsCompleted.ShouldBeFalse();
        source.SignalNext(2);
        lastsTask.IsCompleted.ShouldBeFalse();
        source.SignalNext(3);
        lastsTask.IsCompleted.ShouldBeFalse();
        source.SignalNext(4);
        lastsTask.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();

        var emits = lastsTask.Result;
        emits.Count.ShouldBe(3);
        emits[0].ShouldBe(2);
        emits[1].ShouldBe(3);
        emits[2].ShouldBe(4);
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutEventReturnTimeoutOptionWithoutValueWhenTimeoutEventIsSignalled()
    {
        var source = new TestSource();
        var timeoutEventId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.AddDays(1);

        source.SignalNext(new TimeoutEvent(1.ToEffectId(), expiresAt));
        
        var nextOrSuspend = await source
            .TakeUntilTimeout(timeoutEventId, expiresAt)
            .LastOrNone(maxWait: TimeSpan.Zero);
        
        nextOrSuspend.HasValue.ShouldBeFalse();
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutEventReturnsValueOnSignal()
    {
        var source = new TestSource();
        var timeoutEventId = 1.ToEffectId();
        var expiresAt = DateTime.UtcNow.AddDays(1);

        source.SignalNext(new TimeoutEvent(2.ToEffectId(), expiresAt));
        source.SignalNext("hallo");
        source.SignalNext("world");
        
        var nextOrSuspend = await source
            .TakeUntilTimeout(timeoutEventId, expiresAt)
            .OfType<string>()
            .Take(2)
            .LastOrNone(TimeSpan.Zero);
        
        nextOrSuspend.HasValue.ShouldBeTrue();
        nextOrSuspend.Value.ShouldBe("world");
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionThrowsSuspensionExceptionWhenNothingIsSignaled()
    {
        var source = new TestSource();

        await Should.ThrowAsync<SuspendInvocationException>(
            () => source.OfType<int>().Last(TimeSpan.Zero)
        );
    }
    
    [TestMethod]
    public async Task LastOfTypeReturnsValueOfTypeOnSignal()
    {
        var source = new TestSource();
        
        source.SignalNext(2);
        source.SignalNext("hallo");
        source.SignalNext("world");

        var lastOfType = source.LastOfType<string>();
        lastOfType.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        await BusyWait.Until(() => lastOfType.IsCompletedSuccessfully);
        lastOfType.Result.ShouldBe("world");
    }
    
    [TestMethod]
    public async Task LastOfReturnsValueOfTypeOnSignal()
    {
        var source = new TestSource();
        
        source.SignalNext(2);
        source.SignalNext("hallo");
        source.SignalNext("world");

        var lastOfType = source.LastOf<string>();
        lastOfType.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        await BusyWait.Until(() => lastOfType.IsCompletedSuccessfully);

        lastOfType.Result.ShouldBe("world");
    }
    
    #endregion
    
    #region Completion
    
    [TestMethod]
    public async Task CompletionOperatorCompletesAfterStreamCompletion()
    {
        var source = new TestSource();
            
        var completion = source.Completion();
        completion.IsCompleted.ShouldBeFalse();

        await Task.Delay(10);
        completion.IsCompleted.ShouldBeFalse();
        
        source.SignalNext("hello");
        completion.IsCompleted.ShouldBeFalse();
        
        await Task.Delay(10);
        completion.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        await BusyWait.Until(() => completion.IsCompletedSuccessfully);
    }
    
    [TestMethod]
    public void CompletionOperatorWithSuspensionCompletesImmediatelyOnCompletedStream()
    {
        var source = new TestSource();

        source.SignalNext(1);
        source.SignalNext(2);
        var completionOrSuspend = source.OfType<int>().Take(1).Completion(maxWait: TimeSpan.Zero);
        
        completionOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
    }
    
    [TestMethod]
    public async Task CompletionOperatorWithSuspensionAndTimeoutSucceedsWithImmediateSignal()
    {
        var source = new TestSource();

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        var nextOrSuspend = source.Completion(maxWait: TimeSpan.FromSeconds(1));
        source.SignalNext(1);
        source.SignalNext(2);

        source.SignalCompletion();
        
        await nextOrSuspend;
        stopWatch.Stop();
        
        nextOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
        
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(1));
    }
    
    [TestMethod]
    public async Task CompletionOperatorWithSuspensionAndTimeoutThrowsSuspensionExceptionWhenNothingIsSignalledWithinMaxWaitDelay()
    {
        var source = new TestSource();
        
        var nextOrSuspend = source.Completion(maxWait: TimeSpan.FromMilliseconds(100));

        await Should.ThrowAsync<SuspendInvocationException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task CompletionOperatorWithSuspensionThrowsSuspensionExceptionWhenNothingIsSignaled()
    {
        var source = new TestSource();

        await Should.ThrowAsync<SuspendInvocationException>(
            () => source.OfType<int>().Completion(maxWait: TimeSpan.Zero)
        );
    }
    
    #endregion
    
    #region Error Propagation

    [TestMethod]
    public async Task ThrownExceptionInOperatorResultsInNextLeafThrowingSameException()
    {
        var source = new TestSource();
        var next = source.Where(_ => throw new InvalidOperationException("oh no")).First();
            
        next.IsCompleted.ShouldBeFalse();
        source.SignalNext("hello");
            
        await BusyWait.Until(() => next.IsCompleted);
        
        next.IsFaulted.ShouldBeTrue();
        next.Exception!.InnerException.ShouldBeOfType<InvalidOperationException>();
    }
    
    [TestMethod]
    public async Task ThrownExceptionInOperatorResultsInLastLeafThrowingSameException()
    {
        var source = new TestSource();
        var next = source.Where(_ => throw new InvalidOperationException("oh no")).Last();
            
        next.IsCompleted.ShouldBeFalse();
        source.SignalNext("hello");

        await BusyWait.Until(() => next.IsCompleted);
        next.IsFaulted.ShouldBeTrue();
        next.Exception!.InnerException.ShouldBeOfType<InvalidOperationException>();
    }
    
    [TestMethod]
    public async Task ThrownExceptionInOperatorResultsInCompletionLeafThrowingSameException()
    {
        var source = new TestSource();
        var next = source.Where(_ => throw new InvalidOperationException("oh no")).Completion();
            
        next.IsCompleted.ShouldBeFalse();
        source.SignalNext("hello");
            
        await BusyWait.Until(() => next.IsCompleted);
        next.Exception!.InnerException.ShouldBeOfType<InvalidOperationException>();
    }

    #endregion
    
    #region TryOperators

    [TestMethod]
    public void TryNextReturnsFalseOnNonEmittingStream()
    {
        var source = new TestSource();
        source.SignalNext("hallo");

        var existing = source.OfType<int>().Existing(out var streamCompleted);
        existing.Count.ShouldBe(0);
        streamCompleted.ShouldBeFalse();
    }
    
    [TestMethod]
    public void TryNextReturnsTrueOnEmittingStream()
    {
        var source = new TestSource();
        source.SignalNext("hallo");
        source.SignalNext(2);

        var existing = source
            .OfType<int>()
            .Existing(out var streamCompleted);

        existing.Count.ShouldBe(1);
        existing.Single().ShouldBe(2);
        streamCompleted.ShouldBeFalse();
    }
    
    [TestMethod]
    public void TryLastReturnsNonCompletedStreamOnNonCompletedStream()
    {
        var source = new TestSource();
        source.SignalNext("hallo");

        var existing = source.OfType<int>().Existing(out var completed);
        existing.Count.ShouldBe(0);
        completed.ShouldBeFalse();
    }
    
    [TestMethod]
    public void TryLastReturnsSteamCompletedWithoutValueOnNonEmittingCompletedStream()
    {
        var source = new TestSource();
        source.SignalNext("hallo");

        var existing = source
            .Take(1)
            .OfType<int>()
            .Existing(out var completed);
        
        existing.Count.ShouldBe(0);
        completed.ShouldBe(true);
    }
    
    [TestMethod]
    public void TryLastReturnsStreamCompletedWithValueOnCompletedStream()
    {
        var source = new TestSource();
        source.SignalNext("hallo");
        source.SignalNext(2);

        var existing = source
            .OfType<int>()
            .Take(1)
            .Existing(out var completed);

        existing.Count.ShouldBe(1);
        existing.Single().ShouldBe(2);
        completed.ShouldBeTrue();
    }

    #endregion

    #region PullExisting

    [TestMethod]
    public void PullExistingOnChunkedChainShouldNotReturnPartialChunk()
    {
        var source = new TestSource();
        source.SignalNext("hello");
        var existing = source.Chunk(2).Existing(out _);
        existing.Count.ShouldBe(0);
    }
    
    [TestMethod]
    public void PullExistingShouldReturnAllEmitsSoFar()
    {
        var source = new TestSource();
        source.SignalNext("hello");
        source.SignalNext("world");
        var existing = source.Existing(out _);
        existing.Count.ShouldBe(2);
        existing[0].ShouldBe("hello");
        existing[1].ShouldBe("world");
    }

    #endregion
}