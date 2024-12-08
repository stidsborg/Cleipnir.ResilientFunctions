using Cleipnir.ResilientFunctions.MariaDb.Tests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDB.Tests.RFunctionTests;

[TestClass]
public class SemaphoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.SemaphoreTests
{
    [TestMethod]
    public override Task SunshineTest()
        => SunshineTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task WaitingFlowIsInterruptedAfterSemaphoreBecomesFree()
        => WaitingFlowIsInterruptedAfterSemaphoreBecomesFree(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExistingSemaphoreCanBeForceReleased()
        => ExistingSemaphoreCanBeForceReleased(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task SemaphoreAllowsTwoFlowsToContinueAtTheSameTime()
        => SemaphoreAllowsTwoFlowsToContinueAtTheSameTime(FunctionStoreFactory.Create());
}