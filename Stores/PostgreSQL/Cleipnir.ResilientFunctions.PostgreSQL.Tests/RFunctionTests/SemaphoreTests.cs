using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

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