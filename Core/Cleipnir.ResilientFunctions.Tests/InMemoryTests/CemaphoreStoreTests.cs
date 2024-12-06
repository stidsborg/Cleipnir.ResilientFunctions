using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class CemaphoreStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.CemaphoreStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(FunctionStoreFactory.Create().SelectAsync(s => s.CemaphoreStore));

    [TestMethod]
    public override Task ReleasingCemaphoreTwiceSucceeds()
        => ReleasingCemaphoreTwiceSucceeds(FunctionStoreFactory.Create().SelectAsync(s => s.CemaphoreStore));

    [TestMethod]
    public override Task AcquiringTheSameCemaphoreTwiceIsIdempotent()
        => AcquiringTheSameCemaphoreTwiceIsIdempotent(FunctionStoreFactory.Create().SelectAsync(s => s.CemaphoreStore));
}