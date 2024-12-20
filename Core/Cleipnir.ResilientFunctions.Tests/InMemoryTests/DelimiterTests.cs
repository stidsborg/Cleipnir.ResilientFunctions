using System;
using Cleipnir.ResilientFunctions.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class DelimiterTests
{
    [TestMethod]
    public void FlowInstanceMustNotContainUnitDelimiter()
    {
        var invalidId = "Test" + Delimiters.UnitSeparator;
        Assert.ThrowsException<ArgumentException>(() => new FlowInstance(invalidId));
    }
    
    [TestMethod]
    public void FlowTypeMustNotContainUnitDelimiter()
    {
        var invalidId = "Test" + Delimiters.UnitSeparator;
        Assert.ThrowsException<ArgumentException>(() => new FlowType(invalidId));
    } 
}