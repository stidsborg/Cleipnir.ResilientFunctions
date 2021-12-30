using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.WatchDogsTests;

[TestClass]
public class WatchdogCompoundTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests.WatchdogCompoundTests
{
    [TestMethod]
    public override Task FunctionCompoundTest() 
        => FunctionCompoundTest(CreateFunctionStore());
    
    [TestMethod]
    public override Task FunctionWithScrapbookCompoundTest() 
        => FunctionWithScrapbookCompoundTest(CreateFunctionStore());
    
    [TestMethod]
    public override Task ActionCompoundTest()
        => ActionCompoundTest(CreateFunctionStore());
    
    [TestMethod]
    public override Task ActionWithScrapbookCompoundTest()
        => ActionWithScrapbookCompoundTest(CreateFunctionStore());

    private IFunctionStore CreateFunctionStore([System.Runtime.CompilerServices.CallerMemberName] string callMemberName = "")
        => Sql.CreateAndInitializeStore(nameof(WatchdogCompoundTests), callMemberName).Result;
}