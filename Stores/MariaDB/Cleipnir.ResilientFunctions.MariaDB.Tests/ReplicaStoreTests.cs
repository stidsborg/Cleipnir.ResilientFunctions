using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests;

[TestClass]
public class ReplicaStoreTests : ResilientFunctions.Tests.TestTemplates.ReplicaStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest() 
        => SunshineScenarioTest(FunctionStoreFactory.Create());
}