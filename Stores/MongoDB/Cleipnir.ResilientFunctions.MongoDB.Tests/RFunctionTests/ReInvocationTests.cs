using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class ReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ActionWithScrapbookReInvocationSunshineScenario()
        => ActionWithScrapbookReInvocationSunshineScenario(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterIsPassedInOnReInvocationSunshineScenario(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ScrapbookUpdaterIsCalledBeforeReInvokeOnAction()
        => ScrapbookUpdaterIsCalledBeforeReInvokeOnAction(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc()
        => ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task FuncWithScrapbookReInvocationSunshineScenario()
        => FuncWithScrapbookReInvocationSunshineScenario(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionDoesNotExist()
        => ReInvocationFailsWhenTheFunctionDoesNotExist(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionIsAtUnsupportedVersion()
        => ReInvocationFailsWhenTheFunctionIsAtUnsupportedVersion(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ReInvocationThroughRFunctionsSunshine()
        => ReInvocationThroughRFunctionsSunshine(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ScheduleReInvocationThroughRFunctionsSunshine()
        => ScheduleReInvocationThroughRFunctionsSunshine(NoSql.AutoCreateAndInitializeStore());
}