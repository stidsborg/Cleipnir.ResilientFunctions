using System.Threading.Tasks;
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
            source.SignalNext("hello");
            source.SignalNext("world");

            await Should.ThrowAsync<SuspendInvocationException>(
                () => source.SuspendUntilFirstOfType<int>()
            );
        }
        
        [TestMethod]
        public async Task EventIsEmittedInResultWhenEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            source.SignalNext("hello");
            source.SignalNext(1);
            source.SignalNext("world");

            var next = await source.SuspendUntilFirstOfType<int>();

            next.ShouldBe(1);
        }
        
        [TestMethod]
        public void TrySuspensionIsDetectedWhenNoEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            source.SignalNext("hello");
            source.SignalNext("world");

            var existing = source
                .Existing(out var emittedFromSource);
            
            existing.Count.ShouldBe(2);;
            emittedFromSource.ShouldBe(2);
        }
        
        [TestMethod]
        public void EventIsEmittedInOptionResultWhenEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source(NoOpTimeoutProvider.Instance);
            source.SignalNext("hello");
            source.SignalNext(1);
            source.SignalNext(2);
            source.SignalNext("world");

            var existing = source
                .OfType<int>()
                .Existing(out var emittedFromSource);

            existing.Count.ShouldBe(2);
            existing[0].ShouldBe(1);
            existing[1].ShouldBe(2);
            emittedFromSource.ShouldBe(4);
        }
    }
}