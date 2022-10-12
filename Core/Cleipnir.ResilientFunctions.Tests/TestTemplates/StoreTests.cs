using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class StoreTests
{
    private FunctionId FunctionId { get; } = new FunctionId("functionId", "instanceId");
    private const string PARAM = "param";

    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            FunctionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        var nonCompletes = await store
            .GetExecutingFunctions(FunctionId.TypeId, versionUpperBound: 0)
            .ToTaskAsync();
            
        nonCompletes.Count.ShouldBe(1);
        var nonCompleted = nonCompletes[0];
        nonCompleted.InstanceId.ShouldBe(FunctionId.InstanceId);
        nonCompleted.Epoch.ShouldBe(0);
        nonCompleted.SignOfLife.ShouldBe(0);

        var storedFunction = await store.GetFunction(FunctionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.FunctionId.ShouldBe(FunctionId);
        storedFunction.Parameter.ParamJson.ShouldBe(paramJson);
        storedFunction.Parameter.ParamType.ShouldBe(paramType);
        storedFunction.Scrapbook.ShouldNotBeNull();
        storedFunction.Scrapbook.ScrapbookType.ShouldBe(typeof(RScrapbook).SimpleQualifiedName());
        storedFunction.Epoch.ShouldBe(0);
        storedFunction.SignOfLife.ShouldBe(0);
        storedFunction.PostponedUntil.ShouldBeNull();

        const string result = "hello world";
        var resultJson = result.ToJson();
        var resultType = result.GetType().SimpleQualifiedName();
        await store.SetFunctionState(
            FunctionId,
            Status.Succeeded,
            scrapbookJson: new RScrapbook().ToJson(),
            result: new StoredResult(resultJson, resultType),
            errorJson: null,
            postponedUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();
            
        storedFunction = await store.GetFunction(FunctionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Result.ShouldNotBeNull();
        storedFunction.Result.Deserialize<object>(DefaultSerializer.Instance).ShouldBe(result);
    }

    public abstract Task SignOfLifeIsUpdatedWhenAsExpected();
    protected async Task SignOfLifeIsUpdatedWhenAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            FunctionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store
            .UpdateSignOfLife(FunctionId, expectedEpoch: 0, newSignOfLife: 1)
            .ShouldBeTrueAsync();

        var nonCompletedFunctions = 
            await store.GetExecutingFunctions(FunctionId.TypeId, versionUpperBound: 0);
        var nonCompletedFunction = nonCompletedFunctions.Single();
        nonCompletedFunction.Epoch.ShouldBe(0);
        nonCompletedFunction.SignOfLife.ShouldBe(1);
    }

    public abstract Task SignOfLifeIsNotUpdatedWhenNotAsExpected();
    protected async Task SignOfLifeIsNotUpdatedWhenNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            FunctionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store.UpdateSignOfLife(
            FunctionId,  
            expectedEpoch: 1,
            newSignOfLife: 1
        ).ShouldBeFalseAsync();

        var nonCompletedFunctions = 
            await store.GetExecutingFunctions(FunctionId.TypeId, versionUpperBound: 0);
        var nonCompletedFunction = nonCompletedFunctions.Single();
        nonCompletedFunction.Epoch.ShouldBe(0);
        nonCompletedFunction.SignOfLife.ShouldBe(0);
    }
        
    public abstract Task BecomeLeaderSucceedsWhenEpochIsAsExpected();
    protected async Task BecomeLeaderSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            FunctionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store
            .TryToBecomeLeader(
                FunctionId, 
                Status.Executing, 
                expectedEpoch: 0, 
                newEpoch: 1, 
                crashedCheckFrequency: 100, 
                version: 0
            ).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(FunctionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(1);
        storedFunction.SignOfLife.ShouldBe(0);
    }
        
    public abstract Task BecomeLeaderFailsWhenEpochIsNotAsExpected();
    protected async Task BecomeLeaderFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            FunctionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store.TryToBecomeLeader(
            FunctionId, 
            Status.Executing, 
            expectedEpoch: 0, newEpoch: 2,
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store
            .TryToBecomeLeader(
                FunctionId, 
                Status.Executing, 
                expectedEpoch: 0, 
                newEpoch: 1, 
                crashedCheckFrequency: 100, 
                version: 0
            ).ShouldBeFalseAsync();

        var storedFunction = await store.GetFunction(FunctionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(2);
        storedFunction.SignOfLife.ShouldBe(0);
    }
    
    public abstract Task CreatingTheSameFunctionTwiceReturnsFalse();
    protected async Task CreatingTheSameFunctionTwiceReturnsFalse(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            FunctionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store.CreateFunction(
            FunctionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeFalseAsync();
    }
    
    public abstract Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut();
    protected async Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();
        var nowTicks = DateTime.UtcNow.Ticks;
        
        await store.CreateFunction(
            FunctionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            FunctionId,
            Status.Postponed,
            scrapbookJson: new RScrapbook().ToJson(),
            result: null,
            errorJson: null,
            postponedUntil: nowTicks,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var postponedFunctions = await store.GetPostponedFunctions(
            FunctionId.TypeId,
            expiresBefore: nowTicks - 100,
            versionUpperBound: 0
        );
        postponedFunctions.ShouldBeEmpty();
    }
    
    public abstract Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut();
    protected async Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();
        var nowTicks = DateTime.UtcNow.Ticks;
        
        await store.CreateFunction(
            FunctionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            FunctionId,
            Status.Postponed,
            scrapbookJson: new RScrapbook().ToJson(),
            result: null,
            errorJson: null,
            postponedUntil: nowTicks,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var postponedFunctions = await store.GetPostponedFunctions(
            FunctionId.TypeId,
            expiresBefore: nowTicks + 100,
            versionUpperBound: 0
        );
        postponedFunctions.Count().ShouldBe(1);
    }
    
    public abstract Task InitializeCanBeInvokedMultipleTimesSuccessfully();
    protected async Task InitializeCanBeInvokedMultipleTimesSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.Initialize();
        await store.Initialize();
    }
    
    public abstract Task CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency();
    protected async Task CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = new FunctionId(
            nameof(StoreTests),
            nameof(CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency)
        );
        var crashedCheckFrequency = TimeSpan.FromSeconds(10).Ticks;
        
        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: crashedCheckFrequency,
            version: 0
        );

        var storedFunctions = await store.GetExecutingFunctions(functionId.TypeId, versionUpperBound: 0).ToListAsync();
        storedFunctions.Count.ShouldBe(1);
        var sf = storedFunctions[0];
        sf.CrashedCheckFrequency.ShouldBe(crashedCheckFrequency);
    }
    
    public abstract Task LeaderElectionSpecifiedCrashCheckFrequencyIsSameAsExecutingFunctionCrashCheckFrequency();
    protected async Task LeaderElectionSpecifiedCrashCheckFrequencyIsSameAsExecutingFunctionCrashCheckFrequency(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = new FunctionId(
            nameof(StoreTests),
            nameof(CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency)
        );
        var crashedCheckFrequency = TimeSpan.FromSeconds(10).Ticks;
        
        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks,
            version: 0
        );

        await store.TryToBecomeLeader(
            functionId, 
            Status.Executing, 
            expectedEpoch: 0, 
            newEpoch: 1, 
            crashedCheckFrequency, 
            version: 0
        );
        var storedFunctions = await store.GetExecutingFunctions(functionId.TypeId, versionUpperBound: 0).ToListAsync();
        storedFunctions.Count.ShouldBe(1);
        var sf = storedFunctions[0];
        sf.CrashedCheckFrequency.ShouldBe(crashedCheckFrequency);
    }
}