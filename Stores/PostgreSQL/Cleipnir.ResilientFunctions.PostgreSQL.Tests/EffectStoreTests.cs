using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests;

[TestClass]
public class EffectStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.EffectStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(FunctionStoreFactory.Create().SelectAsync(f => f.EffectsStore));

    [TestMethod]
    public override Task SingleEffectWithResultLifeCycle()
        => SingleEffectWithResultLifeCycle(FunctionStoreFactory.Create().SelectAsync(f => f.EffectsStore));

    [TestMethod]
    public override Task SingleFailingEffectLifeCycle()
        => SingleFailingEffectLifeCycle(FunctionStoreFactory.Create().SelectAsync(f => f.EffectsStore));
    
    [TestMethod]
    public override Task EffectCanBeDeleted()
        => EffectCanBeDeleted(FunctionStoreFactory.Create().SelectAsync(f => f.EffectsStore));

    [TestMethod]
    public override Task DeleteFunctionIdDeletesAllRelatedEffects()
        => DeleteFunctionIdDeletesAllRelatedEffects(FunctionStoreFactory.Create().SelectAsync(f => f.EffectsStore));
}