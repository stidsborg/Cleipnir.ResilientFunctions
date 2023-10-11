using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Awaiter;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class LeafOperatorsTests
{
    [TestMethod]
    public void NextOperatorEmitsFirstEmittedEvent()
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
    public void LastOperatorEmitsLastEmittedEventAfterStreamCompletion()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext(1);
            
        var last = source.Last();
        source.SignalNext(2);
            
        last.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        
        last.IsCompletedSuccessfully.ShouldBeTrue();
        last.Result.ShouldBe(2);
    }
    
    [TestMethod]
    public void CompletionOperatorCompletesAfterStreamCompletion()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
            
        var completion = source.Completion();
        completion.IsCompleted.ShouldBeFalse();
        
        source.SignalNext("hello");
        completion.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        
        completion.IsCompletedSuccessfully.ShouldBeTrue();
    }
    
    [TestMethod]
    public void NextOperatorWithSuspensionEmitsFirstValue()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        source.SignalNext(1);
        source.SignalNext(2);
        var nextOrSuspend = source.OfType<int>().SuspendUntilNext();
        
        nextOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
        nextOrSuspend.Result.ShouldBe(1);
    }
    
    [TestMethod]
    public void LastOperatorWithSuspensionEmitsLastValue()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        source.SignalNext(1);
        source.SignalNext(2);
        var lastOrSuspend = source.OfType<int>().Take(2).SuspendUntilLast();
        
        lastOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
        lastOrSuspend.Result.ShouldBe(2);
    }
    
    [TestMethod]
    public void CompletionOperatorWithSuspensionCompletesImmediatelyOnCompletedStream()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        source.SignalNext(1);
        source.SignalNext(2);
        var completionOrSuspend = source.OfType<int>().Take(1).SuspendUntilCompletion();
        
        completionOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
    }
    
    [TestMethod]
    public async Task NextOperatorWithSuspensionAndTimeoutSucceedsWithImmediateSignal()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        var nextOrSuspend = source.SuspendUntilNext(waitBeforeSuspension: TimeSpan.FromSeconds(1));
        source.SignalNext(1);
        source.SignalNext(2);

        await nextOrSuspend;
        stopWatch.Stop();
        
        nextOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
        nextOrSuspend.Result.ShouldBe(1);
        
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(1));
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutSucceedsWithImmediateSignal()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        var nextOrSuspend = source.SuspendUntilLast(waitBeforeSuspension: TimeSpan.FromSeconds(1));
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
    public async Task CompletionOperatorWithSuspensionAndTimeoutSucceedsWithImmediateSignal()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        var nextOrSuspend = source.SuspendUntilCompletion(waitBeforeSuspension: TimeSpan.FromSeconds(1));
        source.SignalNext(1);
        source.SignalNext(2);

        source.SignalCompletion();
        
        await nextOrSuspend;
        stopWatch.Stop();
        
        nextOrSuspend.IsCompletedSuccessfully.ShouldBeTrue();
        
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(1));
    }
    
    [TestMethod]
    public async Task NextOperatorWithSuspensionAndTimeoutThrowsTimeoutExceptionWhenNothingIsSignalled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        
        var nextOrSuspend = source.SuspendUntilNext(waitBeforeSuspension: TimeSpan.FromMilliseconds(100));
        
        await Should.ThrowAsync<SuspendInvocationException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutThrowsTimeoutExceptionWhenNothingIsSignalled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        
        var nextOrSuspend = source.SuspendUntilLast(waitBeforeSuspension: TimeSpan.FromMilliseconds(100));
        
        await Should.ThrowAsync<SuspendInvocationException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutThrowsNoResultExceptionWhenNothingIsSignalledAndStreamCompletes()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        
        source.SignalNext("hello");
        
        var nextOrSuspend = source
            .Take(1)
            .OfType<int>()
            .SuspendUntilLast(timeoutEventId: "timeoutEventId", timeoutAt: DateTime.UtcNow);
        
        await Should.ThrowAsync<NoResultException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task CompletionOperatorWithSuspensionAndTimeoutThrowsTimeoutExceptionWhenNothingIsSignalled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        
        var nextOrSuspend = source.SuspendUntilCompletion(waitBeforeSuspension: TimeSpan.FromMilliseconds(100));
        
        await Should.ThrowAsync<SuspendInvocationException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task NextOperatorWithSuspensionThrowsSuspensionExceptionWhenNothingIsSignaled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        await Should.ThrowAsync<SuspendInvocationException>(
            () => source.OfType<int>().SuspendUntilNext()
        );
    }
    
    [TestMethod]
    public async Task NextOperatorWithSuspensionAndTimeoutEventThrowsSuspensionExceptionWhenNothingIsSignalled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var timeoutEventId = "TimeoutEventId";
        var expiresAt = DateTime.UtcNow.AddDays(1);
        
        source.SignalNext(new TimeoutEvent("OtherEventId", expiresAt));
        
        var nextOrSuspend = source.OfType<string>().SuspendUntilNext(timeoutEventId, expiresAt);
        
        await Should.ThrowAsync<SuspendInvocationException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutEventThrowsSuspensionExceptionWhenNothingIsSignalled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var timeoutEventId = "TimeoutEventId";
        var expiresAt = DateTime.UtcNow.AddDays(1);
        
        source.SignalNext(new TimeoutEvent("OtherEventId", expiresAt));
        
        var nextOrSuspend = source.SuspendUntilLast(timeoutEventId, expiresAt);
        
        await Should.ThrowAsync<SuspendInvocationException>(nextOrSuspend);
    }
    
    [TestMethod]
    public async Task NextOperatorWithSuspensionAndTimeoutEventReturnTimeoutOptionWithoutValueWhenTimeoutEventIsSignalled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var timeoutEventId = "TimeoutEventId";
        var expiresAt = DateTime.UtcNow.AddDays(1);
        
        source.SignalNext(new TimeoutEvent("TimeoutEventId", expiresAt));
        
        var nextOrSuspend = await source
            .OfType<string>()
            .SuspendUntilNext(timeoutEventId, expiresAt);
        
        nextOrSuspend.TimedOut.ShouldBeTrue();
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutEventReturnTimeoutOptionWithoutValueWhenTimeoutEventIsSignalled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var timeoutEventId = "TimeoutEventId";
        var expiresAt = DateTime.UtcNow.AddDays(1);
        
        source.SignalNext(new TimeoutEvent("TimeoutEventId", expiresAt));
        
        var nextOrSuspend = await source.SuspendUntilLast(timeoutEventId, expiresAt);
        
        nextOrSuspend.TimedOut.ShouldBeTrue();
    }
    
    [TestMethod]
    public async Task NextOperatorWithSuspensionAndTimeoutEventReturnsValueOnSignal()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var timeoutEventId = "TimeoutEventId";
        var expiresAt = DateTime.UtcNow.AddDays(1);
        
        source.SignalNext(new TimeoutEvent("OtherEventId", expiresAt));
        source.SignalNext("hallo");
        source.SignalNext("world");
        
        var nextOrSuspend = await source.OfType<string>().SuspendUntilNext(timeoutEventId, expiresAt);
        
        nextOrSuspend.TimedOut.ShouldBeFalse();
        nextOrSuspend.Value.ShouldBe("hallo");
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionAndTimeoutEventReturnsValueOnSignal()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var timeoutEventId = "TimeoutEventId";
        var expiresAt = DateTime.UtcNow.AddDays(1);
        
        source.SignalNext(new TimeoutEvent("OtherEventId", expiresAt));
        source.SignalNext("hallo");
        source.SignalNext("world");
        
        var nextOrSuspend = await source.OfType<string>().Take(2).SuspendUntilLast(timeoutEventId, expiresAt);
        
        nextOrSuspend.TimedOut.ShouldBeFalse();
        nextOrSuspend.Value.ShouldBe("world");
    }
    
    [TestMethod]
    public async Task LastOperatorWithSuspensionThrowsSuspensionExceptionWhenNothingIsSignaled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        await Should.ThrowAsync<SuspendInvocationException>(
            () => source.OfType<int>().SuspendUntilLast()
        );
    }
    
    [TestMethod]
    public async Task CompletionOperatorWithSuspensionThrowsSuspensionExceptionWhenNothingIsSignaled()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        await Should.ThrowAsync<SuspendInvocationException>(
            () => source.OfType<int>().SuspendUntilCompletion()
        );
    }
    
    [TestMethod]
    public void ThrownExceptionInOperatorResultsInNextLeafThrowingSameException()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var next = source.Where(_ => throw new InvalidOperationException("oh no")).Next();
            
        next.IsCompleted.ShouldBeFalse();
        source.SignalNext("hello");
            
        next.IsFaulted.ShouldBeTrue();
        next.Exception!.InnerException.ShouldBeOfType<InvalidOperationException>();
    }
    
    [TestMethod]
    public void ThrownExceptionInOperatorResultsInLastLeafThrowingSameException()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var next = source.Where(_ => throw new InvalidOperationException("oh no")).Last();
            
        next.IsCompleted.ShouldBeFalse();
        source.SignalNext("hello");
            
        next.IsFaulted.ShouldBeTrue();
        next.Exception!.InnerException.ShouldBeOfType<InvalidOperationException>();
    }
    
    [TestMethod]
    public void ThrownExceptionInOperatorResultsInCompletionLeafThrowingSameException()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var next = source.Where(_ => throw new InvalidOperationException("oh no")).Completion();
            
        next.IsCompleted.ShouldBeFalse();
        source.SignalNext("hello");
            
        next.IsFaulted.ShouldBeTrue();
        next.Exception!.InnerException.ShouldBeOfType<InvalidOperationException>();
    }

    #region TryOperators

    [TestMethod]
    public void TryNextReturnsFalseOnNonEmittingStream()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hallo");

        var success = source.OfType<int>().TryNext(out var next, out var totalEventSourceCount);
        success.ShouldBeFalse();
        totalEventSourceCount.ShouldBe(1);
    }
    
    [TestMethod]
    public void TryNextReturnsTrueOnEmittingStream()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hallo");
        source.SignalNext(2);

        var success = source.OfType<int>().TryNext(out var next, out var totalEventSourceCount);

        success.ShouldBeTrue();
        next.ShouldBe(2);
        totalEventSourceCount.ShouldBe(2);
    }
    
    [TestMethod]
    public void TryLastReturnsFalseOnNonNonCompletedStream()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hallo");

        var success = source.OfType<int>().TryLast(out var next, out var totalEventSourceCount);
        success.ShouldBeFalse();
        totalEventSourceCount.ShouldBe(1);
    }
    
    [TestMethod]
    public void TryLastReturnsTrueOnCompletedStream()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hallo");
        source.SignalNext(2);

        var success = source.OfType<int>().Take(1).TryLast(out var next, out var totalEventSourceCount);

        success.ShouldBeTrue();
        next.ShouldBe(2);
        totalEventSourceCount.ShouldBe(2);
    }

    #endregion

    #region PullExisting

    [TestMethod]
    public void PullExistingOnChunkedChainShouldNotReturnPartialChunk()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        var existing = source.Chunk(2).PullExisting();
        existing.Count.ShouldBe(0);
    }
    
    [TestMethod]
    public void PullExistingShouldReturnAllEmitsSoFar()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        source.SignalNext("world");
        var existing = source.PullExisting();
        existing.Count.ShouldBe(2);
        existing[0].ShouldBe("hello");
        existing[1].ShouldBe("world");
    }

    #endregion
}