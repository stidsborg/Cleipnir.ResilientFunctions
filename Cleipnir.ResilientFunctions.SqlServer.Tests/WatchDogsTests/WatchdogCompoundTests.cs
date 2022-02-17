using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.WatchDogsTests;

[TestClass]
public class WatchdogCompoundTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests.WatchdogCompoundTests
{
    [TestMethod]
    public override Task FunctionCompoundTest() 
        => FunctionCompoundTest(Sql.AutoCreateAndInitializeStore().Result);
    
    [TestMethod]
    public override Task FunctionWithScrapbookCompoundTest() 
        => FunctionWithScrapbookCompoundTest(Sql.AutoCreateAndInitializeStore().Result);
    
    [TestMethod]
    public override Task ActionCompoundTest()
        => ActionCompoundTest(Sql.AutoCreateAndInitializeStore().Result);
    
    [TestMethod]
    public override Task ActionWithScrapbookCompoundTest()
        => ActionWithScrapbookCompoundTest(Sql.AutoCreateAndInitializeStore().Result);
}