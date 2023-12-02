using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class ActivityStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.ActivityStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(new InMemoryActivityStore().CastTo<IActivityStore>().ToTask());

    [TestMethod]
    public override Task SingleActivityWithResultLifeCycle()
        => SingleActivityWithResultLifeCycle(new InMemoryActivityStore().CastTo<IActivityStore>().ToTask());

    [TestMethod]
    public override Task SingleFailingActivityLifeCycle()
        => SingleFailingActivityLifeCycle(new InMemoryActivityStore().CastTo<IActivityStore>().ToTask());

    [TestMethod]
    public override Task ActivitiesCanBeDeleted()
        => ActivitiesCanBeDeleted(new InMemoryActivityStore().CastTo<IActivityStore>().ToTask());
}