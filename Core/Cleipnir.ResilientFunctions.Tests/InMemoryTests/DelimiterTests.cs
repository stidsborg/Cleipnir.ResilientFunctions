using System;
using Cleipnir.ResilientFunctions.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class DelimiterTests
{
    [TestMethod]
    public void FunctionInstanceIdMustNotContainUnitDelimiter()
    {
        var invalidId = "Test" + Delimiters.UnitSeparator;
        Assert.ThrowsException<ArgumentException>(() => new FunctionInstanceId(invalidId));
    }
    
    [TestMethod]
    public void FunctionTypeIdMustNotContainUnitDelimiter()
    {
        var invalidId = "Test" + Delimiters.UnitSeparator;
        Assert.ThrowsException<ArgumentException>(() => new FunctionTypeId(invalidId));
    } 
}