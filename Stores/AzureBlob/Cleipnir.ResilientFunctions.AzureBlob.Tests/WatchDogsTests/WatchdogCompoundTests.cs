using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.WatchDogsTests;

[TestClass]
public class WatchdogCompoundTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests.WatchdogCompoundTests
{
    [TestMethod]
    public override Task FunctionCompoundTest() 
        => FunctionCompoundTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task FunctionWithScrapbookCompoundTest() 
        => FunctionWithScrapbookCompoundTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ActionCompoundTest()
        => ActionCompoundTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ActionWithScrapbookCompoundTest()
        => ActionWithScrapbookCompoundTest(FunctionStoreFactory.Create());
}