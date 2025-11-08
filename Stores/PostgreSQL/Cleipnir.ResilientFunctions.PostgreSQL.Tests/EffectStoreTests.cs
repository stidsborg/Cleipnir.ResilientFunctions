using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests;

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
    
    [TestMethod]
    public override Task BulkInsertTest()
        => BulkInsertTest(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));
    
    [TestMethod]
    public override Task BulkInsertAndDeleteTest()
        => BulkInsertAndDeleteTest(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));
    
    [TestMethod]
    public override Task BulkDeleteTest()
        => BulkDeleteTest(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));
    
    [TestMethod]
    public override Task UpsertEmptyCollectionOfEffectsDoesNotThrowException()
        => UpsertEmptyCollectionOfEffectsDoesNotThrowException(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));
    
    [TestMethod]
    public override Task EffectsForDifferentIdsCanBeFetched()
        => EffectsForDifferentIdsCanBeFetched(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));
    
    [TestMethod]
    public override Task OverwriteExistingEffectWorks()
        => OverwriteExistingEffectWorks(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task StoreCanHandleMultipleEffectsWithSameIdOnDifferentSessions()
        => StoreCanHandleMultipleEffectsWithSameIdOnDifferentSessions(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MultipleSequentialUpdatesWithoutRefresh()
        => MultipleSequentialUpdatesWithoutRefresh(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task StoreHandlesLargeNumberOfEffects()
        => StoreHandlesLargeNumberOfEffects(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task EffectsSerializeAndDeserializeCorrectly()
        => EffectsSerializeAndDeserializeCorrectly(FunctionStoreFactory.Create().SelectAsync(fs => fs.EffectsStore));

    [TestMethod]
    public override Task MixedInsertUpdateDeleteInSequence()
        => MixedInsertUpdateDeleteInSequence(FunctionStoreFactory.Create());
}