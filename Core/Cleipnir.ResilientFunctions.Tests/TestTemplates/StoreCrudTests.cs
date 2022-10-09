﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class StoreCrudTests
{
    private FunctionId FunctionId { get; } = new FunctionId("funcType1", "funcInstance1");
    private TestParameters TestParam { get; } = new TestParameters("Peter", 32);
    private StoredParameter Param => new(TestParam.ToJson(), typeof(TestParameters).SimpleQualifiedName());
    private record TestParameters(string Name, int Age);

    private class TestScrapbook : RScrapbook
    {
        public string? Note { get; set; }
    }
        
    public abstract Task FunctionCanBeCreatedWithASingleParameterSuccessfully();
    protected async Task FunctionCanBeCreatedWithASingleParameterSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(FunctionId);
        stored!.FunctionId.ShouldBe(FunctionId);
        stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
        stored.Parameter.ParamType.ShouldBe(Param.ParamType);
        stored.Scrapbook.ShouldNotBeNull();
        stored.Scrapbook.ScrapbookType.ShouldBe(typeof(RScrapbook).SimpleQualifiedName());
        stored.Result.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.PostponedUntil.ShouldBeNull();
        stored.Epoch.ShouldBe(0);
        stored.SignOfLife.ShouldBe(0);
    }

    public abstract Task FunctionCanBeCreatedWithTwoParametersSuccessfully();
    protected async Task FunctionCanBeCreatedWithTwoParametersSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new TestScrapbook().ToJson(), typeof(TestScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(FunctionId);
        stored!.FunctionId.ShouldBe(FunctionId);
        stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
        stored.Parameter.ParamType.ShouldBe(Param.ParamType);
        stored.Scrapbook.ShouldNotBeNull();
        stored.Scrapbook.ScrapbookJson.ShouldNotBeNull();
        stored.Scrapbook.ScrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
        stored.Result.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.PostponedUntil.ShouldBeNull();
        stored.Epoch.ShouldBe(0);
        stored.SignOfLife.ShouldBe(0);
    }
        
    public abstract Task FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully();
    protected async Task FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new TestScrapbook().ToJson(), typeof(TestScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(FunctionId);
        stored!.FunctionId.ShouldBe(FunctionId);
        stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
        stored.Parameter.ParamType.ShouldBe(Param.ParamType);
        stored.Scrapbook.ShouldNotBeNull();
        stored.Scrapbook.ScrapbookJson.ShouldNotBeNull();
        stored.Scrapbook.ScrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
        stored.Result.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.PostponedUntil.ShouldBeNull();
        stored.Epoch.ShouldBe(0);
        stored.SignOfLife.ShouldBe(0);
    }

    public abstract Task FetchingNonExistingFunctionReturnsNull();
    protected async Task FetchingNonExistingFunctionReturnsNull(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.GetFunction(FunctionId).ShouldBeNullAsync();
    }  
   
    public abstract Task SignOfLifeIsUpdatedWhenCurrentEpochMatches();
    protected async Task SignOfLifeIsUpdatedWhenCurrentEpochMatches(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new TestScrapbook().ToJson(), typeof(TestScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store.UpdateSignOfLife(FunctionId, expectedEpoch: 0, newSignOfLife: 1).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(FunctionId);
        storedFunction!.Epoch.ShouldBe(0);
        storedFunction!.SignOfLife.ShouldBe(1);
    }
    
    public abstract Task SignOfLifeIsNotUpdatedWhenCurrentEpochIsDifferent();
    protected async Task SignOfLifeIsNotUpdatedWhenCurrentEpochIsDifferent(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new TestScrapbook().ToJson(), typeof(TestScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store.UpdateSignOfLife(FunctionId, expectedEpoch: 1, newSignOfLife: 1).ShouldBeFalseAsync();

        var storedFunction = await store.GetFunction(FunctionId);
        storedFunction!.Epoch.ShouldBe(0);
        storedFunction!.SignOfLife.ShouldBe(0);
    }

    public abstract Task UpdateScrapbookSunshineScenario();
    protected async Task UpdateScrapbookSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new TestScrapbook().ToJson(), typeof(TestScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        var scrapbook = new TestScrapbook { Note = "something is still something" };
        await store.SetFunctionState(
            FunctionId,
            Status.Executing,
            scrapbook.ToJson(),
            result: null,
            errorJson: null,
            postponedUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(FunctionId);
        storedFunction!.Scrapbook.ShouldNotBeNull();
        var (scrapbookJson, scrapbookType) = storedFunction.Scrapbook;
            
        scrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
        scrapbookJson.ShouldBe(scrapbook.ToJson());
    }
        
    public abstract Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected();
    protected async Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new TestScrapbook().ToJson(), typeof(TestScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        var scrapbook = new TestScrapbook { Note = "something is still something" };
        await store.SetFunctionState(
            FunctionId,
            Status.Executing,
            scrapbook.ToJson(),
            result: null,
            errorJson: null,
            postponedUntil: null,
            expectedEpoch: 1
        ).ShouldBeFalseAsync();

        var (scrapbookJson, scrapbookType) = (await store.GetFunction(FunctionId))!.Scrapbook!;
        scrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
        scrapbookJson.ShouldNotBeNull();
    }

    public abstract Task ExistingFunctionCanBeDeleted();
    public async Task ExistingFunctionCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new TestScrapbook().ToJson(), typeof(TestScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store.DeleteFunction(FunctionId).ShouldBeTrueAsync();

        await store.GetFunction(FunctionId).ShouldBeNullAsync();
    }
    
    public abstract Task NonExistingFunctionCanBeDeleted();
    public async Task NonExistingFunctionCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.DeleteFunction(FunctionId).ShouldBeFalseAsync();
    }
    
    public abstract Task ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected();
    public async Task ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new TestScrapbook().ToJson(), typeof(TestScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();
        await store.TryToBecomeLeader(
            FunctionId,
            Status.Executing,
            expectedEpoch: 0,
            newEpoch: 1,
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();
        await store.DeleteFunction(FunctionId, expectedEpoch: 0).ShouldBeFalseAsync();

        await store.GetFunction(FunctionId).ShouldNotBeNullAsync();
    }
    
    public abstract Task ExistingFunctionIsNotDeletedWhenStatusIsNotAsExpected();
    public async Task ExistingFunctionIsNotDeletedWhenStatusIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new TestScrapbook().ToJson(), typeof(TestScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();
        
        await store.SetFunctionState(
            FunctionId,
            Status.Succeeded,
            scrapbookJson: new TestScrapbook().ToJson(),
            result: null,
            errorJson: null,
            postponedUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();
        
        await store.DeleteFunction(FunctionId, expectedStatus: Status.Executing).ShouldBeFalseAsync();
        await store.GetFunction(FunctionId).ShouldNotBeNullAsync();
    }
    
    public abstract Task ExistingFunctionIsNotDeletedWhenStatusAndEpochIsNotAsExpected();
    public async Task ExistingFunctionIsNotDeletedWhenStatusAndEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new TestScrapbook().ToJson(), typeof(TestScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();
        
        await store.SetFunctionState(
            FunctionId,
            Status.Succeeded,
            scrapbookJson: new TestScrapbook().ToJson(),
            result: null,
            errorJson: null,
            postponedUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();
        
        await store.DeleteFunction(FunctionId, expectedStatus: Status.Executing, expectedEpoch: 0).ShouldBeFalseAsync();
        await store.GetFunction(FunctionId).ShouldNotBeNullAsync();
    }
}