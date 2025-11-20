using Cleipnir.ResilientFunctions.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class EffectContextTests
{
    [TestMethod]
    public void NewlyCreatedContextHasZeroCounter()
    {
        var context = EffectContext.Empty;
        context.Parent.ShouldBeNull();
        context.NextImplicitId().ShouldBe(0);
    }
    
    [TestMethod]
    public void CounterCanBeIncrementedWithoutCreatingContext()
    {
        var context = EffectContext.Empty;
        context.NextImplicitId();
        context.Parent.ShouldBeNull();
        context.NextImplicitId().ShouldBe(1);
    }
}