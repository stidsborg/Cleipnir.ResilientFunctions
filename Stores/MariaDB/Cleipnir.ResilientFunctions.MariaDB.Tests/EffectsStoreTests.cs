using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests;

[TestClass]
public class EffectsStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.EffectStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SingleEffectWithResultLifeCycle()
        => SingleEffectWithResultLifeCycle(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SingleFailingEffectLifeCycle()
        => SingleFailingEffectLifeCycle(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EffectCanBeDeleted()
        => EffectCanBeDeleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task DeleteFunctionIdDeletesAllRelatedEffects()
        => DeleteFunctionIdDeletesAllRelatedEffects(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task TruncateDeletesAllEffects()
        => TruncateDeletesAllEffects(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task BulkInsertTest()
        => BulkInsertTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task BulkInsertAndDeleteTest()
        => BulkInsertAndDeleteTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task BulkDeleteTest()
        => BulkDeleteTest(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task UpsertEmptyCollectionOfEffectsDoesNotThrowException()
        => UpsertEmptyCollectionOfEffectsDoesNotThrowException(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task EffectsForDifferentIdsCanBeFetched()
        => EffectsForDifferentIdsCanBeFetched(FunctionStoreFactory.Create());
    
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
        => EffectsSerializeAndDeserializeCorrectly(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MixedInsertUpdateDeleteInSequence()
        => MixedInsertUpdateDeleteInSequence(FunctionStoreFactory.Create());
}