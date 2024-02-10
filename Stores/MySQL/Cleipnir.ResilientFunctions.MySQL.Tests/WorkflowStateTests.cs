using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests;

[TestClass]
public class WorkflowStateTests : ResilientFunctions.Tests.TestTemplates.WorkflowStateTests
{
    [TestMethod]
    public override Task SunshineScenario()
        => SunshineScenario(FunctionStoreFactory.Create());

    [TestMethod]
    public override async Task StateIsNotUpdatedWhenEpochIsNotAsExpected()
        => await StateIsNotUpdatedWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ConcreteStateTypeIsUsedWhenSpecifiedAtRegistration()
        => ConcreteStateTypeIsUsedWhenSpecifiedAtRegistration(FunctionStoreFactory.Create());
}