using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class EffectStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.EffectStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(FunctionStoreFactory.CreateEffectStore());

    [TestMethod]
    public override Task SingleEffectWithResultLifeCycle()
        => SingleEffectWithResultLifeCycle(FunctionStoreFactory.CreateEffectStore());

    [TestMethod]
    public override Task SingleFailingEffectLifeCycle()
        => SingleFailingEffectLifeCycle(FunctionStoreFactory.CreateEffectStore());

    [TestMethod]
    public override Task EffectCanBeDeleted()
        => EffectCanBeDeleted(FunctionStoreFactory.CreateEffectStore());

    [TestMethod]
    public override Task DeleteFunctionIdDeletesAllRelatedEffects()
        => DeleteFunctionIdDeletesAllRelatedEffects(FunctionStoreFactory.CreateEffectStore());

    [TestMethod]
    public override Task TruncateDeletesAllEffects()
        => TruncateDeletesAllEffects(FunctionStoreFactory.CreateEffectStore());
}