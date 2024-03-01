using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class EffectStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.EffectStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(new InMemoryEffectsStore().CastTo<IEffectsStore>().ToTask());

    [TestMethod]
    public override Task SingleEffectWithResultLifeCycle()
        => SingleEffectWithResultLifeCycle(new InMemoryEffectsStore().CastTo<IEffectsStore>().ToTask());

    [TestMethod]
    public override Task SingleFailingEffectLifeCycle()
        => SingleFailingEffectLifeCycle(new InMemoryEffectsStore().CastTo<IEffectsStore>().ToTask());

    [TestMethod]
    public override Task EffectCanBeDeleted()
        => EffectCanBeDeleted(new InMemoryEffectsStore().CastTo<IEffectsStore>().ToTask());
}