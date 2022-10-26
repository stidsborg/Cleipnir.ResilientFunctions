using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class ScheduleReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ScheduleReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(NoSql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ActionWithScrapbookReInvocationSunshineScenario()
        => ActionWithScrapbookReInvocationSunshineScenario(NoSql.AutoCreateAndInitializeStore());
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
    public override Task ReInvocationSucceedsDespiteUnexpectedStatusWhenNotThrowOnUnexpectedFunctionState()
        => ReInvocationSucceedsDespiteUnexpectedStatusWhenNotThrowOnUnexpectedFunctionState(NoSql.AutoCreateAndInitializeStore());
}