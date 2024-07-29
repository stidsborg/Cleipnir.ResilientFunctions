using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class InnerOperatorsTests
{
    #region Select
    
    [TestMethod]
    public async Task SelectOperatorProjectsEvent()
    {
        var source = new TestSource();
        
        var emits = source
            .OfType<string>()
            .Select(s => s.Length)
            .Completion();
        
        source.SignalNext("a", new InterruptCount(1));
        source.SignalNext("ab", new InterruptCount(2));
        source.SignalNext("abc", new InterruptCount(3));
        source.SignalNext("abcd", new InterruptCount(4));
        
        emits.IsCompleted.ShouldBeFalse();
        source.SignalCompletion();

        await BusyWait.UntilAsync(() => emits.IsCompletedSuccessfully);
        emits.IsCompletedSuccessfully.ShouldBeTrue();
        
        var l = emits.Result;
        l.SequenceEqual(new[] { 1, 2, 3, 4 }).ShouldBeTrue();
    }
    
    #endregion

    #region Where

    [TestMethod]
    public async Task WhereOperatorFiltersOutEmitsAsPerPredicate()
    {
        var source = new TestSource();
        
        var emits = source.OfType<int>().Where(n => n > 2).ToList();
        
        source.SignalNext(1, new InterruptCount(1));
        source.SignalNext(2, new InterruptCount(2));
        source.SignalNext(3, new InterruptCount(3));
        source.SignalNext(4, new InterruptCount(4));
        emits.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        await BusyWait.UntilAsync(() => emits.IsCompletedSuccessfully);
        
        var l = emits.Result;
        l.Count.ShouldBe(2);
        l[0].ShouldBe(3);
        l[1].ShouldBe(4);
    }

    #endregion

    #region Take & Skip

    [TestMethod]
    public async Task SubscriptionWithSkip1CompletesAfterNonSkippedSubscription()
    {
        var source = new TestSource();
        var next1 = source.First();
        var next2 = source.Skip(1).First();
            
        source.SignalNext("hello", new InterruptCount(1));
        
        await BusyWait.UntilAsync(() => next1.IsCompletedSuccessfully);
        next2.IsCompleted.ShouldBeFalse();
        
        source.SignalNext("world", new InterruptCount(2));

        await BusyWait.UntilAsync(() => next2.IsCompletedSuccessfully);
    }
    
    [TestMethod]
    public async Task TakeUntilCompletesAfterPredicateIsMet()
    {
        var source = new TestSource();

        var emits = source
            .OfType<int>()
            .TakeUntil(i => i > 2)
            .ToList();
            
        source.SignalNext(1, new InterruptCount(1));
        source.SignalNext(2, new InterruptCount(2));

        await Task.Delay(10);
        emits.IsCompleted.ShouldBeFalse();
        
        source.SignalNext(3, new InterruptCount(3));
        await BusyWait.UntilAsync(() => emits.IsCompletedSuccessfully);

        var l = emits.Result;
        l.Count.ShouldBe(2);
        l[0].ShouldBe(1);
        l[1].ShouldBe(2);
    }
    
    [TestMethod]
    public async Task SkipUntilSkipsUntilPredicateIsMet()
    {
        var source = new TestSource();

        var emits = source
            .OfType<int>()
            .SkipUntil(i => i > 2)
            .ToList();
        
        source.SignalNext(1, new InterruptCount(1));
        source.SignalNext(2, new InterruptCount(2));

        await Task.Delay(10);
        emits.IsCompleted.ShouldBeFalse();
        
        source.SignalNext(3, new InterruptCount(3));
        source.SignalNext(2, new InterruptCount(4));
        
        source.SignalCompletion();

        await BusyWait.UntilAsync(() => emits.IsCompletedSuccessfully);
        
        var l = emits.Result;
        l.Count.ShouldBe(2);
        l[0].ShouldBe(3);
        l[1].ShouldBe(2);
    }

    [TestMethod]
    public async Task TakeOperatorCompletesAfterInput()
    {
        var source = new TestSource();
        source.SignalNext("hello", new InterruptCount(1));
        
        var task = source.Take(2).ToList();

        source.SignalNext("world", new InterruptCount(2));

        await BusyWait.UntilAsync(() => task.IsCompletedSuccessfully);

        var emits = task.Result;
        emits.Count.ShouldBe(2);
        emits[0].ShouldBe("hello");
        emits[1].ShouldBe("world");
    }

    #endregion

    // *** SCAN *** //
    [TestMethod]
    public async Task ScanOperatorEmitsIntermediaryStateOnEachEmitTakeOperatorCompletesAfterInput()
    {
        var source = new TestSource();

        source.SignalNext(1, new InterruptCount(1));
        var intermediaryEmits = new List<int>();
        var lastTask = source
            .OfType<int>()
            .Scan(seed: 0, (akk, n) => akk + n)
            .Select(runningTotal =>
            {
                intermediaryEmits.Add(runningTotal);
                return runningTotal;
            })
            .Last();

        await BusyWait.UntilAsync(() => intermediaryEmits.Count == 1);
        intermediaryEmits[0].ShouldBe(1);
        lastTask.IsCompleted.ShouldBeFalse();
        
        source.SignalNext(1, new InterruptCount(2));
        await BusyWait.UntilAsync(() => intermediaryEmits.Count == 2);
        intermediaryEmits[1].ShouldBe(2);

        source.SignalCompletion();
        
        await BusyWait.UntilAsync(() => lastTask.IsCompletedSuccessfully);
        
        intermediaryEmits.Count.ShouldBe(2);
        lastTask.IsCompletedSuccessfully.ShouldBeTrue();
        lastTask.Result.ShouldBe(2);
    }
    
    #region OfTypes

    [TestMethod]
    public async Task EventsCanBeFilteredByType()
    {
        var source = new TestSource();
        var nextStringEmitted = source.OfType<string>().First();
        await Task.Delay(10);
        nextStringEmitted.IsCompleted.ShouldBeFalse();

        await Task.Delay(10);
        source.SignalNext(1, new InterruptCount(1));
        nextStringEmitted.IsCompleted.ShouldBeFalse();

        source.SignalNext("hello", new InterruptCount(2));

        await BusyWait.UntilAsync(() => nextStringEmitted.IsCompletedSuccessfully);
        nextStringEmitted.IsCompleted.ShouldBeTrue();
        nextStringEmitted.Result.ShouldBe("hello");
    }
    
    [TestMethod]
    public void OfTwoTypesTest()
    {
        var source = new TestSource();
        source.SignalNext("hello", new InterruptCount(1));
        source.SignalNext(2, new InterruptCount(2));
        
        {
            var either = source.OfTypes<string, int>().First().Result;
            either.ValueSpecified.ShouldBe(Either<string, int>.Value.First);
            either.HasFirst.ShouldBeTrue();
            either.Do(first: s => s.ShouldBe("hello"), second: _ => throw new Exception("Unexpected value"));
            var matched = either.Match(first: s => s.ToUpper(), second: _ => throw new Exception("Unexpected value"));
            matched.ShouldBe("HELLO");
        }

        {
            var either = source.Skip(1).OfTypes<string, int>().First().Result;
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
        var source = new TestSource();
        source.SignalNext("hello", new InterruptCount(1));
        source.SignalNext(2, new InterruptCount(2));
        source.SignalNext(25L, new InterruptCount(3));
        
        {
            var either = source.OfTypes<string, int, long>().First().Result;
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
            var either = source.Skip(2).OfTypes<string, int, long>().First().Result;
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
    
    #endregion

    #region Buffer / Chunk

    [TestMethod]
    public async Task BufferOperatorTest()
    {
        var source = new TestSource();
        source.SignalNext("hello", new InterruptCount(1));

        var nextTask = source.Buffer(2).First();
        var listTask = source.Buffer(2).ToList();
        
        nextTask.IsCompleted.ShouldBeFalse();
        listTask.IsCompleted.ShouldBeFalse();
        source.SignalNext("world", new InterruptCount(2));
        
        await BusyWait.UntilAsync(() => nextTask.IsCompletedSuccessfully);
        
        var result = await nextTask;
        result.Count.ShouldBe(2);
        result[0].ShouldBe("hello");
        result[1].ShouldBe("world");

        source.SignalNext("hello", new InterruptCount(3));
        source.SignalNext("universe", new InterruptCount(4));
        source.SignalCompletion();

        await BusyWait.UntilAsync(() => listTask.IsCompletedSuccessfully);
        
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
        var source = new TestSource();
        source.SignalNext("hello", new InterruptCount(1));

        var nextTask = source.Buffer(2).First();
        
        source.SignalCompletion();

        await BusyWait.UntilAsync(() => nextTask.IsCompleted);
        nextTask.IsCompletedSuccessfully.ShouldBeTrue();
        var emitted = await nextTask;
        emitted.Count.ShouldBe(1);
        emitted[0].ShouldBe("hello");
    }

    #endregion

    #region DistinctBy

    [TestMethod]
    public async Task DistinctBySuccessfullyFiltersOutDuplicates()
    {
        var source = new TestSource();
        source.SignalNext("hello", new InterruptCount(1));
        source.SignalNext("hello", new InterruptCount(2));

        var task = source
            .OfType<string>()
            .DistinctBy(s => s)
            .Take(2)
            .Completion();

        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();
        
        source.SignalNext("world", new InterruptCount(3));
        
        await BusyWait.UntilAsync(() => task.IsCompletedSuccessfully);
        
        var emitted = await task;
        emitted.Count.ShouldBe(2);
        emitted[0].ShouldBe("hello");
        emitted[1].ShouldBe("world");
    }

    #endregion
    
    #region WithCallback

    [TestMethod]
    public async Task CallbackOperatorIsInvokedOnEachEmit()
    {
        var source = new TestSource();
        source.SignalNext("hello", new InterruptCount(1));
        source.SignalNext("world", new InterruptCount(2));

        var emittedSoFar = new List<string>();
        var task = source
            .OfType<string>()
            .Take(4)
            .Callback(s => emittedSoFar.Add(s))
            .Completion();
        
        emittedSoFar.Count.ShouldBe(2);
        emittedSoFar[0].ShouldBe("hello");
        emittedSoFar[1].ShouldBe("world");

        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();
        
        source.SignalNext("and", new InterruptCount(3));
        source.SignalNext("universe", new InterruptCount(4));
        
        await BusyWait.UntilAsync(() => emittedSoFar.Count == 4);
        
        emittedSoFar.Count.ShouldBe(4);
        emittedSoFar[2].ShouldBe("and");
        emittedSoFar[3].ShouldBe("universe");

        await BusyWait.UntilAsync(() => task.IsCompletedSuccessfully);
        
        source.SignalNext("and", new InterruptCount(4));
        source.SignalNext("multiverse", new InterruptCount(5));
        
        await BusyWait.UntilAsync(() => emittedSoFar.Count == 4);
    }

    #endregion
}