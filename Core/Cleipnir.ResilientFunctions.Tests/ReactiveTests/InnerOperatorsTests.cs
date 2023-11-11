using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Origin;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class InnerOperatorsTests
{
    #region Select
    
    [TestMethod]
    public void SelectOperatorProjectsEvent()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        
        var emits = source
            .OfType<string>()
            .Select(s => s.Length)
            .Completion();
        
        source.SignalNext("a");
        source.SignalNext("ab");
        source.SignalNext("abc");
        source.SignalNext("abcd");
        
        emits.IsCompleted.ShouldBeFalse();
        source.SignalCompletion();
        emits.IsCompletedSuccessfully.ShouldBeTrue();
        
        var l = emits.Result;
        l.SequenceEqual(new[] { 1, 2, 3, 4 }).ShouldBeTrue();
    }
    
    #endregion

    #region Where

    [TestMethod]
    public void WhereOperatorFiltersOutEmitsAsPerPredicate()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        
        var emits = source.OfType<int>().Where(n => n > 2).ToList();
        
        source.SignalNext(1);
        source.SignalNext(2);
        source.SignalNext(3);
        source.SignalNext(4);
        emits.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        emits.IsCompletedSuccessfully.ShouldBeTrue();
        var l = emits.Result;
        l.Count.ShouldBe(2);
        l[0].ShouldBe(3);
        l[1].ShouldBe(4);
    }

    #endregion

    #region Take & Skip

    [TestMethod]
    public void SubscriptionWithSkip1CompletesAfterNonSkippedSubscription()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var next1 = source.First();
        var next2 = source.Skip(1).First();
            
        source.SignalNext("hello");
        next1.IsCompletedSuccessfully.ShouldBeTrue();
        next2.IsCompleted.ShouldBeFalse();
        source.SignalNext("world");
        next2.IsCompletedSuccessfully.ShouldBeTrue();
    }
    
    [TestMethod]
    public void TakeUntilCompletesAfterPredicateIsMet()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        var emits = source
            .OfType<int>()
            .TakeUntil(i => i > 2)
            .ToList();
            
        source.SignalNext(1);
        emits.IsCompleted.ShouldBeFalse();
        
        source.SignalNext(2);
        emits.IsCompleted.ShouldBeFalse();
        
        source.SignalNext(3);
        emits.IsCompletedSuccessfully.ShouldBeTrue();

        var l = emits.Result;
        l.Count.ShouldBe(2);
        l[0].ShouldBe(1);
        l[1].ShouldBe(2);
    }
    
    [TestMethod]
    public void SkipUntilSkipsUntilPredicateIsMet()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        var emits = source
            .OfType<int>()
            .SkipUntil(i => i > 2)
            .ToList();
        
        source.SignalNext(1);
        emits.IsCompleted.ShouldBeFalse();
        
        source.SignalNext(2);
        emits.IsCompleted.ShouldBeFalse();
        
        source.SignalNext(3);
        emits.IsCompleted.ShouldBeFalse();

        source.SignalNext(2);
        emits.IsCompleted.ShouldBeFalse();
        
        source.SignalCompletion();
        emits.IsCompletedSuccessfully.ShouldBeTrue();
        
        var l = emits.Result;
        l.Count.ShouldBe(2);
        l[0].ShouldBe(3);
        l[1].ShouldBe(2);
    }

    [TestMethod]
    public void TakeOperatorCompletesAfterInput()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        
        var takes = source.Take(2).ToList();
        takes.IsCompleted.ShouldBeFalse();    

        source.SignalNext("world");
        
        takes.IsCompletedSuccessfully.ShouldBeTrue();

        var emits = takes.Result;
        emits.Count.ShouldBe(2);
        emits[0].ShouldBe("hello");
        emits[1].ShouldBe("world");
    }

    #endregion

    // *** SCAN *** //
    [TestMethod]
    public void ScanOperatorEmitsIntermediaryStateOnEachEmitTakeOperatorCompletesAfterInput()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        source.SignalNext(1);
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
        
        intermediaryEmits.Count.ShouldBe(1);
        intermediaryEmits[0].ShouldBe(1);
        lastTask.IsCompleted.ShouldBeFalse();
        
        source.SignalNext(1);
        intermediaryEmits.Count.ShouldBe(2);
        intermediaryEmits[1].ShouldBe(2);

        source.SignalCompletion();
        
        intermediaryEmits.Count.ShouldBe(2);
        lastTask.IsCompletedSuccessfully.ShouldBeTrue();
        lastTask.Result.ShouldBe(2);
    }

    #region Merge

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
    
    #endregion

    #region OfTypes

        [TestMethod]
    public void EventsCanBeFilteredByType()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        var nextStringEmitted = source.OfType<string>().First();
        nextStringEmitted.IsCompleted.ShouldBeFalse();
            
        source.SignalNext(1);
        nextStringEmitted.IsCompleted.ShouldBeFalse();

        source.SignalNext("hello");

        nextStringEmitted.IsCompleted.ShouldBeTrue();
        nextStringEmitted.Result.ShouldBe("hello");
    }
    
    [TestMethod]
    public void OfTwoTypesTest()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        source.SignalNext(2);
        
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
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        source.SignalNext(2);
        source.SignalNext(25L);
        
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
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");

        var nextTask = source.Buffer(2).First();
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

        var nextTask = source.Buffer(2).First();
        
        source.SignalCompletion();
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
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        source.SignalNext("hello");

        var task = source
            .OfType<string>()
            .DistinctBy(s => s)
            .Take(2)
            .Completion();
        
        task.IsCompleted.ShouldBeFalse();
        
        source.SignalNext("world");
        
        task.IsCompletedSuccessfully.ShouldBeTrue();
        var emitted = await task;
        emitted.Count.ShouldBe(2);
        emitted[0].ShouldBe("hello");
        emitted[1].ShouldBe("world");
    }

    #endregion
    
    #region WithCallback

    [TestMethod]
    public void CallbackOperatorIsInvokedOnEachEmit()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");
        source.SignalNext("world");

        var emittedSoFar = new List<string>();
        var task = source
            .OfType<string>()
            .Take(4)
            .Callback(s => emittedSoFar.Add(s))
            .Completion();
        
        emittedSoFar.Count.ShouldBe(2);
        emittedSoFar[0].ShouldBe("hello");
        emittedSoFar[1].ShouldBe("world");
        
        task.IsCompleted.ShouldBeFalse();
        
        source.SignalNext("and");
        source.SignalNext("universe");
        
        emittedSoFar.Count.ShouldBe(4);
        emittedSoFar[2].ShouldBe("and");
        emittedSoFar[3].ShouldBe("universe");
        
        task.IsCompletedSuccessfully.ShouldBeTrue();
        
        source.SignalNext("and");
        source.SignalNext("multiverse");
        
        emittedSoFar.Count.ShouldBe(4);
    }

    #endregion
}