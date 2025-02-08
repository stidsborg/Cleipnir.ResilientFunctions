using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests;

[TestClass]
public class EffectsStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.EffectStoreTests
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
    
    [TestMethod]
    public override Task BulkInsertTest()
        => BulkInsertTest(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));
    
    [TestMethod]
    public override Task BulkInsertAndDeleteTest()
        => BulkInsertAndDeleteTest(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));
    
    [TestMethod]
    public override Task BulkDeleteTest()
        => BulkDeleteTest(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));
}