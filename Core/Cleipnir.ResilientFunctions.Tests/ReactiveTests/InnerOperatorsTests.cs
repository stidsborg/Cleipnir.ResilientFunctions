using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Reactive;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class InnerOperatorsTests
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
    public void MergeTests()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);

        var toUpper = source.OfType<string>().Select(s => s.ToUpper());

        var emitsTask = source.Merge(toUpper).Take(2).Lasts();
        
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
    public async Task BufferOperatorTest()
    {
        var source = new Source(NoOpTimeoutProvider.Instance);
        source.SignalNext("hello");

        var nextTask = source.Buffer(2).Next();
        var listTask = source.Buffer(2).Lasts();
        
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