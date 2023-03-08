using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class ReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task ActionWithScrapbookReInvocationSunshineScenario()
        => ActionWithScrapbookReInvocationSunshineScenario(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterIsPassedInOnReInvocationSunshineScenario(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task ScrapbookUpdaterIsCalledBeforeReInvokeOnAction()
        => ScrapbookUpdaterIsCalledBeforeReInvokeOnAction(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc()
        => ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task FuncWithScrapbookReInvocationSunshineScenario()
        => FuncWithScrapbookReInvocationSunshineScenario(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionDoesNotExist()
        => ReInvocationFailsWhenTheFunctionDoesNotExist(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task ReInvocationThroughRFunctionsSunshine()
        => ReInvocationThroughRFunctionsSunshine(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task ScheduleReInvocationThroughRFunctionsSunshine()
        => ScheduleReInvocationThroughRFunctionsSunshine(FunctionStoreFactory.FunctionStoreTask);
}