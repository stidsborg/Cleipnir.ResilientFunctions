using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Reactive;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests
{
    [TestClass]
    public class SuspensionTests
    {
        [TestMethod]
        public async Task SuspensionExceptionIsThrownWhenNoEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            source.SignalNext("hello");
            source.SignalNext("world");

            await Should.ThrowAsync<SuspendInvocationException>(
                () => source.SuspendUntilNextOfType<int>()
            );
        }
        
        [TestMethod]
        public async Task EventIsEmittedInResultWhenEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            source.SignalNext("hello");
            source.SignalNext(1);
            source.SignalNext("world");

            var next = await source.SuspendUntilNextOfType<int>();

            next.ShouldBe(1);
        }
        
        [TestMethod]
        public void TrySuspensionIsDetectedWhenNoEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            source.SignalNext("hello");
            source.SignalNext("world");
            
            var success = source.TryNextOfType<int>(out var next, out var totalEventSourceCount);

            success.ShouldBeFalse();
            totalEventSourceCount.ShouldBe(2);
        }
        
        [TestMethod]
        public void EventIsEmittedInOptionResultWhenEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            source.SignalNext("hello");
            source.SignalNext(1);
            source.SignalNext(2);
            source.SignalNext("world");

            var success = source.TryNextOfType<int>(out var next, out var totalEventSourceCount);

            success.ShouldBeTrue();
            next.ShouldBe(1);
            totalEventSourceCount.ShouldBe(4);
        }
    }
}