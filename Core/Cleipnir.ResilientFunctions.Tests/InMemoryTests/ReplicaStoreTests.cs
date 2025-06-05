using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class ReplicaStoreTests : TestTemplates.ReplicaStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(FunctionStoreFactory.Create());
}