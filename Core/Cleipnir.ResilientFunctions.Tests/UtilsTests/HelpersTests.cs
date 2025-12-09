using System;
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

    [TestMethod]
    public void SimpleQualifiedNameForSimpleType()
    {
        var name = typeof(string).SimpleQualifiedName();
        name.ShouldBe("System.String, System.Private.CoreLib");
        Type.GetType(name).ShouldBe(typeof(string));
    }

    [TestMethod]
    public void SimpleQualifiedNameForGenericType()
    {
        var result = typeof(List<string>).SimpleQualifiedName();
        result.ShouldBe("System.Collections.Generic.List`1[[System.String, System.Private.CoreLib]], System.Private.CoreLib");
        Type.GetType(result).ShouldBe(typeof(List<string>));
    }

    [TestMethod]
    public void SimpleQualifiedNameForNestedGenericType()
    {
        var result = typeof(Dictionary<string, List<int>>).SimpleQualifiedName();
        result.ShouldBe(
            "System.Collections.Generic.Dictionary`2[[System.String, System.Private.CoreLib],[System.Collections.Generic.List`1[[System.Int32, System.Private.CoreLib]], System.Private.CoreLib]], System.Private.CoreLib"
        );
        Type.GetType(result).ShouldBe(typeof(Dictionary<string, List<int>>));
    }

    [TestMethod]
    public void SimpleQualifiedNameForArrayType()
    {
        var result = typeof(int[]).SimpleQualifiedName();
        result.ShouldBe("System.Int32[], System.Private.CoreLib");
        Type.GetType(result).ShouldBe(typeof(int[]));
    }

    [TestMethod]
    public void SimpleQualifiedNameForMultiDimensionalArray()
    {
        var name = typeof(int[,]).SimpleQualifiedName();
        name.ShouldBe("System.Int32[,], System.Private.CoreLib");
        Type.GetType(name).ShouldBe(typeof(int[,]));
    }

    [TestMethod]
    public void SimpleQualifiedNameForGenericArrayType()
    {
        var name = typeof(List<string>[]).SimpleQualifiedName();
        name.ShouldBe("System.Collections.Generic.List`1[[System.String, System.Private.CoreLib]][], System.Private.CoreLib");
        Type.GetType(name).ShouldBe(typeof(List<string>[]));
    }

    [TestMethod]
    public void SimpleQualifiedNamePreservesTypeAndAssemblyName()
    {
        var name = typeof(HelpersTests).SimpleQualifiedName();
        name.ShouldBe("Cleipnir.ResilientFunctions.Tests.UtilsTests.HelpersTests, Cleipnir.ResilientFunctions.Tests");
        Type.GetType(name).ShouldBe(typeof(HelpersTests));
    }

    [TestMethod]
    public void SimpleQualifiedNameForNullableType()
    {
        var name = typeof(int?).SimpleQualifiedName();
        name.ShouldBe("System.Nullable`1[[System.Int32, System.Private.CoreLib]], System.Private.CoreLib");
        Type.GetType(name).ShouldBe(typeof(int?));
    }
}