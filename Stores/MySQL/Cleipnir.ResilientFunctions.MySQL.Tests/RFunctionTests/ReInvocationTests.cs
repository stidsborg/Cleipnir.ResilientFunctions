using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.RFunctionTests;

[TestClass]
public class ReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task ActionWithScrapbookReInvocationSunshineScenario()
        => ActionWithScrapbookReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterIsPassedInOnReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task ScrapbookUpdaterIsCalledBeforeReInvokeOnAction()
        => ScrapbookUpdaterIsCalledBeforeReInvokeOnAction(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc()
        => ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task FuncWithScrapbookReInvocationSunshineScenario()
        => FuncWithScrapbookReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionDoesNotExist()
        => ReInvocationFailsWhenTheFunctionDoesNotExist(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task ReInvocationThroughRFunctionsSunshine()
        => ReInvocationThroughRFunctionsSunshine(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task ScheduleReInvocationThroughRFunctionsSunshine()
        => ScheduleReInvocationThroughRFunctionsSunshine(FunctionStoreFactory.Create());
}