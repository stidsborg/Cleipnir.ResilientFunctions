using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class ReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task ActionWithStateReInvocationSunshineScenario()
        => ActionWithStateReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterIsPassedInOnReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task StateUpdaterIsCalledBeforeReInvokeOnAction()
        => StateUpdaterIsCalledBeforeReInvokeOnAction(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task StateUpdaterIsCalledBeforeReInvokeOnFunc()
        => StateUpdaterIsCalledBeforeReInvokeOnFunc(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task FuncWithStateReInvocationSunshineScenario()
        => FuncWithStateReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionDoesNotExist()
        => ReInvocationFailsWhenTheFunctionDoesNotExist(FunctionStoreFactory.Create());
}