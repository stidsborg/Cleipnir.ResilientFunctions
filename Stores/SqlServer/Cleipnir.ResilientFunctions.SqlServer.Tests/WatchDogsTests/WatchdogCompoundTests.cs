using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.WatchDogsTests;

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