using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Reactive;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests
{
    [TestClass]
    public class OnEachTests
    {
        [TestMethod]
        public void OnEachOperatorIsInvokedForEachSourceEmit()
        {
            var captures = new List<int>();

            var source = new Source(NoOpTimeoutProvider.Instance);
            var subscription = source.OfType<int>().OnEach(captures.Add);
            
            source.SignalNext(1);
            source.SignalNext(2);
            captures.SequenceEqual(Enumerable.Range(1, 2)).ShouldBeTrue();
            
            subscription.Dispose();
            
            source.SignalNext(3);
            captures.SequenceEqual(Enumerable.Range(1, 2)).ShouldBeTrue();
        }
    }
}