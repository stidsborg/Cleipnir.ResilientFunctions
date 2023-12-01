using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class ActivityTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ActivityTests
{
    [TestMethod]
    public override Task SunshineActionTest()
        => SunshineActionTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineAsyncActionTest()
        => SunshineAsyncActionTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineFuncTest()
        => SunshineFuncTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SunshineAsyncFuncTest()
        => SunshineAsyncFuncTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExceptionThrowingActionTest()
        => ExceptionThrowingActionTest(FunctionStoreFactory.Create());
}