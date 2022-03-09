using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ScheduleReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ScheduleReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task ActionWithScrapbookReInvocationSunshineScenario()
        => ActionWithScrapbookReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task FuncWithScrapbookReInvocationSunshineScenario()
        => FuncWithScrapbookReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task ReInvocationFailsWhenItHasUnexpectedStatus()
        => ReInvocationFailsWhenItHasUnexpectedStatus(CreateInMemoryStore());
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionDoesNotExist()
        => ReInvocationFailsWhenTheFunctionDoesNotExist(CreateInMemoryStore());

    private Task<IFunctionStore> CreateInMemoryStore() 
        => new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask();
}