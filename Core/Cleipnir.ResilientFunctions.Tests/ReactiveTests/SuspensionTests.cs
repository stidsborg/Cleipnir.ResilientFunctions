using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Origin;
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
            source.SignalNext("hello", new InterruptCount(1));
            source.SignalNext("world", new InterruptCount(2));

            await Should.ThrowAsync<SuspendInvocationException>(
                () => source.SuspendUntilFirstOfType<int>()
            );
        }
        
        [TestMethod]
        public async Task EventIsEmittedInResultWhenEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            source.SignalNext("hello", new InterruptCount(1));
            source.SignalNext(1, new InterruptCount(2));
            source.SignalNext("world", new InterruptCount(3));

            var next = await source.SuspendUntilFirstOfType<int>();

            next.ShouldBe(1);
        }
        
        [TestMethod]
        public void TrySuspensionIsDetectedWhenNoEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            source.SignalNext("hello", new InterruptCount(1));
            source.SignalNext("world", new InterruptCount(2));

            var existing = source.Existing(out var streamCompleted);
            
            existing.Count.ShouldBe(2);;
            streamCompleted.ShouldBeFalse();
        }
        
        [TestMethod]
        public void EventIsEmittedInOptionResultWhenEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            source.SignalNext("hello", new InterruptCount(1));
            source.SignalNext(1, new InterruptCount(2));
            source.SignalNext(2, new InterruptCount(3));
            source.SignalNext("world", new InterruptCount(4));

            var existing = source
                .OfType<int>()
                .Existing(out var streamCompleted);

            existing.Count.ShouldBe(2);
            existing[0].ShouldBe(1);
            existing[1].ShouldBe(2);
            streamCompleted.ShouldBeFalse();
        }
    }
}