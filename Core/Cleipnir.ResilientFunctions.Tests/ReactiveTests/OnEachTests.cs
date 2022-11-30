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

            var source = new Source<int>();
            var subscription = source.OnEach(captures.Add);
            
            source.Emit(1);
            source.Emit(2);
            captures.SequenceEqual(Enumerable.Range(1, 2)).ShouldBeTrue();
            
            subscription.Dispose();
            
            source.Emit(3);
            captures.SequenceEqual(Enumerable.Range(1, 2)).ShouldBeTrue();
        }
    }
}