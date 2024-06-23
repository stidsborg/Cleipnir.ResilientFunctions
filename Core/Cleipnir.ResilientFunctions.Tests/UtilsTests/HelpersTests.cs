using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class HelpersTests
{
    [TestMethod]
    public void AllElementsExistsWhenCreateEnumerableWithRandomOffset()
    {
        var randomOffsetEnumerable = Enumerable.Range(0, count: 10)
            .ToList()
            .WithRandomOffset();

        var set = randomOffsetEnumerable.ToHashSet();
        
        Enumerable
            .Range(0, count: 10)
            .All(i => set.Contains(i))
            .ShouldBeTrue();
    }
    
    [TestMethod]
    public void RandomOffsetCanBeAppliedToEmptyList()
    {
        new List<int>().WithRandomOffset().Any().ShouldBeFalse();
    }
}