using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class ReplicaStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.ReplicaStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(FunctionStoreFactory.Create().SelectAsync(s => s.ReplicaStore));
}