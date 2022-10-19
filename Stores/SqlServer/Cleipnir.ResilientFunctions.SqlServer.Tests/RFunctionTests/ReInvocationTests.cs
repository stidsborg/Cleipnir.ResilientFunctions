﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class ReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ActionWithScrapbookReInvocationSunshineScenario()
        => ActionWithScrapbookReInvocationSunshineScenario(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterIsPassedInOnReInvocationSunshineScenario(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ScrapbookUpdaterIsCalledBeforeReInvokeOnAction()
        => ScrapbookUpdaterIsCalledBeforeReInvokeOnAction(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc()
        => ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task FuncWithScrapbookReInvocationSunshineScenario()
        => FuncWithScrapbookReInvocationSunshineScenario(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ReInvocationFailsWhenItHasUnexpectedStatus()
        => ReInvocationFailsWhenItHasUnexpectedStatus(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionDoesNotExist()
        => ReInvocationFailsWhenTheFunctionDoesNotExist(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionIsAtUnsupportedVersion()
        => ReInvocationFailsWhenTheFunctionIsAtUnsupportedVersion(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ReInvocationThroughRFunctionsSunshine()
        => ReInvocationThroughRFunctionsSunshine(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ScheduleReInvocationThroughRFunctionsSunshine()
        => ScheduleReInvocationThroughRFunctionsSunshine(Sql.AutoCreateAndInitializeStore());
}