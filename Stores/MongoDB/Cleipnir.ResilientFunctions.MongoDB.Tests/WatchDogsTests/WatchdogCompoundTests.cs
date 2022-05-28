using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.WatchDogsTests;

[TestClass]
public class WatchdogCompoundTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests.WatchdogCompoundTests
{
    [TestMethod]
    public override Task FunctionCompoundTest() 
        => FunctionCompoundTest(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task FunctionWithScrapbookCompoundTest() 
        => FunctionWithScrapbookCompoundTest(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ActionCompoundTest()
        => ActionCompoundTest(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ActionWithScrapbookCompoundTest()
        => ActionWithScrapbookCompoundTest(NoSql.AutoCreateAndInitializeStore());
}