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
        var storedParameter = new StoredParameter(paramJson, paramType);
        var storedScrapbook = new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName());
        
        await store.CreateFunction(
            FunctionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        var nonCompletes = await store
            .GetExecutingFunctions(FunctionId.TypeId)
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
        await store.SucceedFunction(
            FunctionId,
            result: new StoredResult(resultJson, resultType),
            scrapbookJson: new RScrapbook().ToJson(),
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult()
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
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store
            .UpdateSignOfLife(FunctionId, expectedEpoch: 0, newSignOfLife: 1, new ComplimentaryState.UpdateSignOfLife())
            .ShouldBeTrueAsync();

        var nonCompletedFunctions = 
            await store.GetExecutingFunctions(FunctionId.TypeId);
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
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store.UpdateSignOfLife(
            FunctionId,  
            expectedEpoch: 1,
            newSignOfLife: 1,
            new ComplimentaryState.UpdateSignOfLife()
        ).ShouldBeFalseAsync();

        var nonCompletedFunctions = 
            await store.GetExecutingFunctions(FunctionId.TypeId);
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
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store
            .RestartExecution(
                FunctionId,
                expectedEpoch: 0,
                crashedCheckFrequency: 100
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
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store
            .RestartExecution(
                FunctionId,
                expectedEpoch: 1,
                crashedCheckFrequency: 100
            ).ShouldBeFalseAsync();

        var storedFunction = await store.GetFunction(FunctionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(0);
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
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store.CreateFunction(
            FunctionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
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
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            FunctionId,
            postponeUntil: nowTicks,
            scrapbookJson: new RScrapbook().ToJson(),
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var postponedFunctions = await store.GetPostponedFunctions(
            FunctionId.TypeId,
            expiresBefore: nowTicks - 100
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
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            FunctionId,
            postponeUntil: nowTicks,
            scrapbookJson: new RScrapbook().ToJson(),
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var postponedFunctions = await store.GetPostponedFunctions(
            FunctionId.TypeId,
            expiresBefore: nowTicks + 100
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
            crashedCheckFrequency: crashedCheckFrequency
        );

        var storedFunctions = await store.GetExecutingFunctions(functionId.TypeId).ToListAsync();
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
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        );

        await store.RestartExecution(
            functionId,
            expectedEpoch: 0,
            crashedCheckFrequency
        );
        var storedFunctions = await store.GetExecutingFunctions(functionId.TypeId).ToListAsync();
        storedFunctions.Count.ShouldBe(1);
        var sf = storedFunctions[0];
        sf.CrashedCheckFrequency.ShouldBe(crashedCheckFrequency);
    }
    
    public abstract Task IncrementEpochSucceedsWhenEpochIsAsExpected();
    protected async Task IncrementEpochSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = new FunctionId(
            nameof(StoreTests),
            nameof(IncrementEpochSucceedsWhenEpochIsAsExpected)
        );

        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        await store.IncrementAlreadyPostponedFunctionEpoch(functionId, expectedEpoch: 0).ShouldBeTrueAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(1);
    }
    
    public abstract Task IncrementEpochFailsWhenEpochIsNotAsExpected();
    protected async Task IncrementEpochFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = new FunctionId(
            nameof(StoreTests),
            nameof(IncrementEpochFailsWhenEpochIsNotAsExpected)
        );

        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        await store.IncrementAlreadyPostponedFunctionEpoch(functionId, expectedEpoch: 1).ShouldBeFalseAsync();
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
    }
}