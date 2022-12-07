using System;
using Cleipnir.ResilientFunctions.Reactive;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests
{
    [TestClass]
    public class LinqTests
    {
        [TestMethod]
        public void EventsCanBeFilteredByType()
        {
            var source = new Source<object>();
            var nextStringEmitted = source.OfType<string>().Next();
            nextStringEmitted.IsCompleted.ShouldBeFalse();
            
            source.Emit(1);
            nextStringEmitted.IsCompleted.ShouldBeFalse();

            source.Emit("hello");

            nextStringEmitted.IsCompleted.ShouldBeTrue();
            nextStringEmitted.Result.ShouldBe("hello");
        }
        
        [TestMethod]
        public void NextOperatorEmitsLastEmittedEventAfterCompletionOfTheStream()
        {
            var source = new Source<int>();
            source.Emit(1);
            
            var next = source.Next();
            source.Emit(2);
            
            next.IsCompletedSuccessfully.ShouldBeTrue();
            next.Result.ShouldBe(1);
            
            source.Emit(3); //should not thrown an error
        }

        [TestMethod]
        public void ThrownExceptionInOperatorResultsInLeafThrowingSameException()
        {
            var source = new Source<string>();
            var next = source.Where(_ => throw new InvalidOperationException("oh no")).Next();
            
            next.IsCompleted.ShouldBeFalse();
            source.Emit("hello");
            
            next.IsFaulted.ShouldBeTrue();
            next.Exception!.InnerException.ShouldBeOfType<InvalidOperationException>();
        }
        
        [TestMethod]
        public void SubscriptionWithSkip1CompletesAfterNonSkippedSubscription()
        {
            var source = new Source<string>();
            var next1 = source.Next();
            var next2 = source.Skip(1).Next();
            
            source.Emit("hello");
            next1.IsCompletedSuccessfully.ShouldBeTrue();
            next2.IsCompleted.ShouldBeFalse();
            source.Emit("world");
            next2.IsCompletedSuccessfully.ShouldBeTrue();
        }
        
        [TestMethod]
        public void StreamCanBeReplayedToCertainEventCountSuccessfully()
        {
            var source = new Source<string>();
            source.Emit("hello");
            source.Emit("world");

            var completed = false;
            var failed = false;
            var latest = "";
            var subscription = source.Subscribe(
                onNext: s => latest = s,
                onCompletion: () => completed = true,
                onError: _ => failed = true
            );

            completed.ShouldBeFalse();
            failed.ShouldBeFalse();
            latest.ShouldBe("");
            
            subscription.ReplayUntil(1);
            
            completed.ShouldBeFalse();
            failed.ShouldBeFalse();
            latest.ShouldBe("hello");
        }
        
        [TestMethod]
        public void StreamCanBeReplayedToCertainEventCountWhenCompletedEarlySuccessfully()
        {
            var source = new Source<string>();
            source.Emit("hello");
            source.Emit("world");

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
            
            subscription.ReplayUntil(2);
            
            completed.ShouldBeTrue();
            failed.ShouldBeFalse();
            latest.ShouldBe("hello");
        }
        
        [TestMethod]
        public void StreamCanBeReplayedToCertainEventCountWhenFailedEarlySuccessfully()
        {
            var source = new Source<string>();
            source.Emit("hello");
            source.Emit("world");

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
            
            subscription.ReplayUntil(2);
            
            completed.ShouldBeFalse();
            failed.ShouldBeTrue();
            latest.ShouldBe("");
        }
    }
}