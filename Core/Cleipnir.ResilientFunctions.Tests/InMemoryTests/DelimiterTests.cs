using System;
using Cleipnir.ResilientFunctions.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class DelimiterTests
{
    [TestMethod]
    public void flowInstanceMustNotContainUnitDelimiter()
    {
        var invalidId = "Test" + Delimiters.UnitSeparator;
        Assert.ThrowsException<ArgumentException>(() => new FlowInstance(invalidId));
    }
    
    [TestMethod]
    public void flowTypeMustNotContainUnitDelimiter()
    {
        var invalidId = "Test" + Delimiters.UnitSeparator;
        Assert.ThrowsException<ArgumentException>(() => new FlowType(invalidId));
    } 
    
    [TestMethod]
    public void EffectIdIdMustNotContainUnitDelimiter()
    {
        var invalidId = "Test" + Delimiters.UnitSeparator;
        Assert.ThrowsException<ArgumentException>(() => new EffectId(invalidId));
    } 
}