﻿using System.Linq;
using System.Threading.Tasks;
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
    public async Task FunctionCanBeCreatedWithASingleParameterSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            scrapbookType: null,
            Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0,
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(FunctionId);
        stored!.FunctionId.ShouldBe(FunctionId);
        stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
        stored.Parameter.ParamType.ShouldBe(Param.ParamType);
        stored.Scrapbook.ShouldBeNull();
        stored.Result.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.PostponedUntil.ShouldBeNull();
        stored.Epoch.ShouldBe(0);
        stored.SignOfLife.ShouldBe(0);
    }

    public abstract Task FunctionCanBeCreatedWithATwoParametersSuccessfully();
    public async Task FunctionCanBeCreatedWithATwoParametersSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            scrapbookType: null,
            Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0,
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(FunctionId);
        stored!.FunctionId.ShouldBe(FunctionId);
        stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
        stored.Parameter.ParamType.ShouldBe(Param.ParamType);
        stored.Scrapbook.ShouldBeNull();
        stored.Result.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.PostponedUntil.ShouldBeNull();
        stored.Epoch.ShouldBe(0);
        stored.SignOfLife.ShouldBe(0);
    }
        
    public abstract Task FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully();
    public async Task FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            scrapbookType: typeof(TestScrapbook).SimpleQualifiedName(),
            Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0,
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(FunctionId);
        stored!.FunctionId.ShouldBe(FunctionId);
        stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
        stored.Parameter.ParamType.ShouldBe(Param.ParamType);
        stored.Scrapbook.ShouldNotBeNull();
        stored.Scrapbook.ScrapbookJson.ShouldBeNull();
        stored.Scrapbook.ScrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
        stored.Result.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.PostponedUntil.ShouldBeNull();
        stored.Epoch.ShouldBe(0);
        stored.SignOfLife.ShouldBe(0);
    }

    public abstract Task FetchingNonExistingFunctionReturnsNull();
    public async Task FetchingNonExistingFunctionReturnsNull(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.GetFunction(FunctionId).ShouldBeNullAsync();
    }  
        
    public abstract Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected();
    public async Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            scrapbookType: null,
            Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0,
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store.UpdateSignOfLife(FunctionId, expectedEpoch: 0, newSignOfLife: 1).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(FunctionId);
        storedFunction!.Epoch.ShouldBe(0);
    }

    public abstract Task UpdateScrapbookSunshineScenario();
    public async Task UpdateScrapbookSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            scrapbookType: typeof(TestScrapbook).SimpleQualifiedName(),
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0,
            crashedCheckFrequency: 100
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
    public async Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            scrapbookType: typeof(TestScrapbook).SimpleQualifiedName(),
            Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0,
            crashedCheckFrequency: 100
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
        scrapbookJson.ShouldBeNull();
    }
}