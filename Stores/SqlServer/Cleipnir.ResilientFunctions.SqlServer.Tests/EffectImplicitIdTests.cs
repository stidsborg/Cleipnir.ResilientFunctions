using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class EffectImplicitIdTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.EffectImplicitIdTests
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

    [TestMethod]
    public override Task TaskWhenAnyFuncTest()
        => TaskWhenAnyFuncTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task TaskWhenAllFuncTest()
        => TaskWhenAllFuncTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task MultipleEffectsTest()
        => MultipleEffectsTest(FunctionStoreFactory.Create());
}