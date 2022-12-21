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
            var source = new Source<object>();
            source.SignalNext("hello");
            source.SignalNext("world");

            await Should.ThrowAsync<SuspendInvocationException>(
                () => source
                    .OfType<int>()
                    .NextOrSuspend()
            );
        }
        
        [TestMethod]
        public async Task EventIsEmittedInResultWhenEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source<object>();
            source.SignalNext("hello");
            source.SignalNext(1);
            source.SignalNext("world");

            var next = await source
                .OfType<int>()
                .NextOrSuspend();
            
            next.ShouldBe(1);
        }
        
        [TestMethod]
        public async Task TrySuspensionIsDetectedWhenNoEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source<object>();
            source.SignalNext("hello");
            source.SignalNext("world");

            var result = await source
                .OfType<int>()
                .TryNextOrSuspend();
            
            result.Outcome.ShouldBe(Outcome.Suspend);
            result.Suspend!.UntilEventSourceCount.ShouldBe(2);
        }
        
        [TestMethod]
        public async Task EventIsEmittedInOptionResultWhenEventHasBeenEmittedFromLeafOperator()
        {
            var source = new Source<object>();
            source.SignalNext("hello");
            source.SignalNext(1);
            source.SignalNext("world");

            var next = await source
                .OfType<int>()
                .TryNextOrSuspend();
            
            next.Outcome.ShouldBe(Outcome.Succeed);
            next.SucceedWithValue.ShouldBe(1);
        }
    }
}