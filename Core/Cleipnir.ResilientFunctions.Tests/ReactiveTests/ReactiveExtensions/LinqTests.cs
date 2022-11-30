using Cleipnir.ResilientFunctions.Reactive;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests.ReactiveExtensions
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
            next.Result.ShouldBe(2);
            
            source.Emit(3); //should not thrown an error
        }
    }
}