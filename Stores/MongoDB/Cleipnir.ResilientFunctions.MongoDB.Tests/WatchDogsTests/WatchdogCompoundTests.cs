using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.WatchDogsTests;

[TestClass]
public class WatchdogCompoundTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests.WatchdogCompoundTests
{
    [TestMethod]
    public override Task FunctionCompoundTest() 
        => FunctionCompoundTest(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task FunctionWithScrapbookCompoundTest() 
        => FunctionWithScrapbookCompoundTest(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ActionCompoundTest()
        => ActionCompoundTest(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ActionWithScrapbookCompoundTest()
        => ActionWithScrapbookCompoundTest(Sql.AutoCreateAndInitializeStore());
}