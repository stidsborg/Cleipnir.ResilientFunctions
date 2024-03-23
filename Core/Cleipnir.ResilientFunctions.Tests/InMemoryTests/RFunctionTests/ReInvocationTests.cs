using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task ActionWithStateReInvocationSunshineScenario()
        => ActionWithStateReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterIsPassedInOnReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task FuncWithStateReInvocationSunshineScenario()
        => FuncWithStateReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionDoesNotExist()
        => ReInvocationFailsWhenTheFunctionDoesNotExist(CreateInMemoryStore());

    private Task<IFunctionStore> CreateInMemoryStore() 
        => FunctionStoreFactory.Create();
}