using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.WatchDogsTests;

[TestClass]
public class WatchdogCompoundTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests.WatchdogCompoundTests
{
    [TestMethod]
    public override Task FunctionCompoundTest() 
        => FunctionCompoundTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task FunctionWithStateCompoundTest() 
        => FunctionWithStateCompoundTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ActionCompoundTest()
        => ActionCompoundTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ActionWithStateCompoundTest()
        => ActionWithStateCompoundTest(FunctionStoreFactory.Create());
}