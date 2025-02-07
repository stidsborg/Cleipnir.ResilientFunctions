using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class EffectStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.EffectStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));

    [TestMethod]
    public override Task SingleEffectWithResultLifeCycle()
        => SingleEffectWithResultLifeCycle(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));

    [TestMethod]
    public override Task SingleFailingEffectLifeCycle()
        => SingleFailingEffectLifeCycle(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));

    [TestMethod]
    public override Task EffectCanBeDeleted()
        => EffectCanBeDeleted(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));

    [TestMethod]
    public override Task DeleteFunctionIdDeletesAllRelatedEffects()
        => DeleteFunctionIdDeletesAllRelatedEffects(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));

    [TestMethod]
    public override Task TruncateDeletesAllEffects()
        => TruncateDeletesAllEffects(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));

    [TestMethod]
    public override Task BulkInsertTest()
        => BulkInsertTest(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));

    [TestMethod]
    public override Task BulkInsertAndDeleteTest()
        => BulkInsertAndDeleteTest(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));
}