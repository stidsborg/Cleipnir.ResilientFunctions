using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests
{
    [TestClass]
    public class AwaitableTests
    {
        [TestMethod]
        public async Task StreamResultIsReflectedInAwaitable()
        {
            var source = new TestSource();

            static async Task<int> Do(IReactiveChain<int> s) => await s.Last();

            var t = Do(source.OfType<int>());
            
            t.IsCompleted.ShouldBeFalse();
            
            source.SignalNext(3, new InterruptCount(1));
            source.SignalNext(5, new InterruptCount(2));
            
            source.SignalCompletion();

            await BusyWait.UntilAsync(() => t.IsCompleted);
            
            t.IsCompleted.ShouldBeTrue();
            t.Result.ShouldBe(5);
        }

        [TestMethod]
        public async Task StreamResultThrowsExceptionWhenNoResultIsReceivedBeforeCompletion()
        {
            var source = new TestSource();
            var taken1 = source.Take(1);

            static async Task<int> Do(IReactiveChain<int> s) => await s.Last();

            var t = Do(source.OfType<int>());

            t.IsCompleted.ShouldBeFalse();

            source.SignalCompletion();

            var completed = false;
            var subscription = taken1.Subscribe(_ => { }, () => completed = true, _ => { });
            await subscription.SyncStore(TimeSpan.Zero);
            subscription.PushMessages();

            await BusyWait.UntilAsync(() => t.IsCompleted);
            completed.ShouldBeTrue();
            
            t.IsCompleted.ShouldBeTrue();

            t.TaskShouldThrow<NoResultException>();
        }

        [TestMethod]
        public async Task StreamResultThrowsExceptionWhenExceptionHasBeenSignaled()
        {
            var source = new TestSource();

            static async Task<int> Do(IReactiveChain<int> s) => await s.Last();

            var t = Do(source.OfType<int>());

            t.IsCompleted.ShouldBeFalse();
            
            source.SignalNext(5, new InterruptCount(1));

            source.SignalError(new TestException());

            await BusyWait.UntilAsync(() => t.IsCompleted);
            t.IsCompleted.ShouldBeTrue();

            t.TaskShouldThrow<TestException>();
        }

        private class TestException : Exception { }
    }
}
