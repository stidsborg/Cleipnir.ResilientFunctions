using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class ScheduleReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ScheduleReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task ActionWithScrapbookReInvocationSunshineScenario()
        => ActionWithScrapbookReInvocationSunshineScenario(Sql.AutoCreateAndInitializeStore());
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
    public override Task ReInvocationSucceedsDespiteUnexpectedStatusWhenNotThrowOnUnexpectedFunctionState()
        => ReInvocationSucceedsDespiteUnexpectedStatusWhenNotThrowOnUnexpectedFunctionState(Sql.AutoCreateAndInitializeStore());
}