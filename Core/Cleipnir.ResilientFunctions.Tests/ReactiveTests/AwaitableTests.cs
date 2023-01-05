using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Awaiter;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests
{
    [TestClass]
    public class AwaitableTests
    {
        [TestMethod]
        public void StreamResultIsReflectedInAwaitable()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);

            static async Task<int> Do(IStream<int> s) => await s.Last();

            var t = Do(source.OfType<int>());
            
            t.IsCompleted.ShouldBeFalse();
            
            source.SignalNext(3);
            source.SignalNext(5);
            
            source.SignalCompletion();

            t.IsCompleted.ShouldBeTrue();
            t.Result.ShouldBe(5);
        }

        [TestMethod]
        public void StreamResultThrowsExceptionWhenNoResultIsReceivedBeforeCompletion()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            var taken1 = source.Take(1);

            static async Task<int> Do(IStream<int> s) => await s.Last();

            var t = Do(source.OfType<int>());

            t.IsCompleted.ShouldBeFalse();

            source.SignalCompletion();

            var completed = false;
            var subscription = taken1.Subscribe(_ => { }, () => completed = true, _ => { });
            subscription.DeliverExistingAndFuture();

            completed.ShouldBeTrue();
            
            t.IsCompleted.ShouldBeTrue();

            t.TaskShouldThrow<NoResultException>();
        }

        [TestMethod]
        public void StreamResultThrowsExceptionWhenExceptionHasBeenSignaled()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);

            static async Task<int> Do(IStream<int> s) => await s.Last();

            var t = Do(source.OfType<int>());

            t.IsCompleted.ShouldBeFalse();
            
            source.SignalNext(5);

            source.SignalError(new TestException());
            
            t.IsCompleted.ShouldBeTrue();

            t.TaskShouldThrow<TestException>();
        }

        private class TestException : Exception { }
    }
}
