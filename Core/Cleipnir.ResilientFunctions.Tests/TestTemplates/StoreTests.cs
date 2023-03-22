using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class StoreTests
{
    private const string PARAM = "param";

    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();
        var storedParameter = new StoredParameter(paramJson, paramType);
        var storedScrapbook = new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName());
        
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => 
            store.GetExecutingFunctions(functionId.TypeId).SelectAsync(efs => efs.Any())
        );
        
        var nonCompletes = await store.GetExecutingFunctions(functionId.TypeId).ToListAsync();
            
        nonCompletes.Count.ShouldBe(1);
        var nonCompleted = nonCompletes[0];
        nonCompleted.InstanceId.ShouldBe(functionId.InstanceId);
        nonCompleted.Epoch.ShouldBe(0);
        nonCompleted.SignOfLife.ShouldBe(0);
        nonCompleted.CrashedCheckFrequency.ShouldBe(100);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.FunctionId.ShouldBe(functionId);
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
            functionId,
            result: new StoredResult(resultJson, resultType),
            scrapbookJson: new RScrapbook().ToJson(),
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
        ).ShouldBeTrueAsync();
            
        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Result.ShouldNotBeNull();
        storedFunction.Result.Deserialize<object>(DefaultSerializer.Instance).ShouldBe(result);
    }

    public abstract Task SignOfLifeIsUpdatedWhenAsExpected();
    protected async Task SignOfLifeIsUpdatedWhenAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store
            .UpdateSignOfLife(functionId, expectedEpoch: 0, newSignOfLife: 1, new ComplimentaryState.UpdateSignOfLife())
            .ShouldBeTrueAsync();

        await BusyWait.Until(() =>
            store.GetExecutingFunctions(functionId.TypeId).SelectAsync(efs => efs.Any())
        );
        
        var nonCompletedFunctions = 
            await store.GetExecutingFunctions(functionId.TypeId);
        var nonCompletedFunction = nonCompletedFunctions.Single();
        nonCompletedFunction.Epoch.ShouldBe(0);
        nonCompletedFunction.SignOfLife.ShouldBe(1);
    }

    public abstract Task SignOfLifeIsNotUpdatedWhenNotAsExpected();
    protected async Task SignOfLifeIsNotUpdatedWhenNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store.UpdateSignOfLife(
            functionId,  
            expectedEpoch: 1,
            newSignOfLife: 1,
            new ComplimentaryState.UpdateSignOfLife()
        ).ShouldBeFalseAsync();

        await BusyWait.Until(() =>
            store.GetExecutingFunctions(functionId.TypeId).SelectAsync(efs => efs.Any())
        );
        
        var nonCompletedFunctions = 
            await store.GetExecutingFunctions(functionId.TypeId);
        
        var nonCompletedFunction = nonCompletedFunctions.Single();
        nonCompletedFunction.Epoch.ShouldBe(0);
        nonCompletedFunction.SignOfLife.ShouldBe(0);
    }
        
    public abstract Task BecomeLeaderSucceedsWhenEpochIsAsExpected();
    protected async Task BecomeLeaderSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store
            .RestartExecution(
                functionId,
                expectedEpoch: 0,
                crashedCheckFrequency: 100
            ).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(1);
        storedFunction.SignOfLife.ShouldBe(0);
    }
        
    public abstract Task BecomeLeaderFailsWhenEpochIsNotAsExpected();
    protected async Task BecomeLeaderFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store
            .RestartExecution(
                functionId,
                expectedEpoch: 1,
                crashedCheckFrequency: 100
            ).ShouldBeFalseAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(0);
        storedFunction.SignOfLife.ShouldBe(0);
    }

    public abstract Task CreatingTheSameFunctionTwiceReturnsFalse();
    protected async Task CreatingTheSameFunctionTwiceReturnsFalse(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
        ).ShouldBeFalseAsync();
    }
    
    public abstract Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut();
    protected async Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();

        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();
        var nowTicks = DateTime.UtcNow.Ticks;

        var storedParameter = new StoredParameter(paramJson, paramType);
        var storedScrapbook = new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName());
        
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store
            .GetPostponedFunctions(functionId.TypeId, expiresBefore: nowTicks + 100)
            .SelectAsync(pfs => pfs.Any())
        );
        
        var postponedFunctions = await store.GetPostponedFunctions(
            functionId.TypeId,
            expiresBefore: nowTicks - 100
        );
        postponedFunctions.ShouldBeEmpty();
    }
    
    public abstract Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut();
    protected async Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();

        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();
        var nowTicks = DateTime.UtcNow.Ticks;
        
        var storedParameter = new StoredParameter(paramJson, paramType);
        var storedScrapbook = new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName());
        
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store
            .GetPostponedFunctions(functionId.TypeId, nowTicks + 100)
            .SelectAsync(pfs => pfs.Any())
        );
        
        var postponedFunctions = await store.GetPostponedFunctions(
            functionId.TypeId,
            expiresBefore: nowTicks + 100
        );
        postponedFunctions.Count().ShouldBe(1);
    }
    
    public abstract Task PostponeFunctionFailsWhenEpochIsNotAsExpected();
    protected async Task PostponeFunctionFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();

        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();
        var nowTicks = DateTime.UtcNow.Ticks;

        var storedParameter = new StoredParameter(paramJson, paramType);
        var storedScrapbook = new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName());
        
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: 1,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
        ).ShouldBeFalseAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        sf.Status.ShouldBe(Status.Executing);
        DefaultSerializer.Instance
            .DeserializeParameter<string>(sf.Parameter.ParamJson, sf.Parameter.ParamType)
            .ShouldBe(PARAM);

        DefaultSerializer.Instance
            .DeserializeScrapbook<RScrapbook>(sf.Scrapbook.ScrapbookJson, sf.Scrapbook.ScrapbookType)
            .ShouldNotBeNull();
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
        var functionId = TestFunctionId.Create();
        var crashedCheckFrequency = TimeSpan.FromSeconds(10).Ticks;
        
        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: crashedCheckFrequency
        );

        await BusyWait.Until(() => store.GetExecutingFunctions(functionId.TypeId).Any());
        
        var storedFunctions = await store.GetExecutingFunctions(functionId.TypeId).ToListAsync();
        storedFunctions.Count.ShouldBe(1);
        var sf = storedFunctions[0];
        sf.CrashedCheckFrequency.ShouldBe(crashedCheckFrequency);
    }
    
    public abstract Task LeaderElectionSpecifiedCrashCheckFrequencyIsSameAsExecutingFunctionCrashCheckFrequency();
    protected async Task LeaderElectionSpecifiedCrashCheckFrequencyIsSameAsExecutingFunctionCrashCheckFrequency(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
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
        
        await BusyWait.Until(() => store.GetExecutingFunctions(functionId.TypeId).Any());
        
        var storedFunctions = await TaskLinq.ToListAsync(store.GetExecutingFunctions(functionId.TypeId));
        storedFunctions.Count.ShouldBe(1);
        var sf = storedFunctions[0];
        sf.CrashedCheckFrequency.ShouldBe(crashedCheckFrequency);
    }
    
    public abstract Task IncrementEpochSucceedsWhenEpochIsAsExpected();
    protected async Task IncrementEpochSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

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
        var functionId = TestFunctionId.Create();

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

    public abstract Task SaveScrapbookOfExecutingFunctionSucceedsWhenEpochIsAsExpected();
    public async Task SaveScrapbookOfExecutingFunctionSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        storedScrapbook = storedScrapbook with { ScrapbookJson = new Scrapbook() { State = "completed" }.ToJson()};
        await store.SaveScrapbookForExecutingFunction(
            functionId,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState.SaveScrapbookForExecutingFunction(storedParameter, storedScrapbook, CrashedCheckFrequency: 0)
        ).ShouldBeTrueAsync();
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook<Scrapbook>(sf.Scrapbook.ScrapbookJson, sf.Scrapbook.ScrapbookType);
        scrapbook.State.ShouldBe("completed");
    }
    
    public abstract Task SaveScrapbookOfExecutingFunctionFailsWhenEpochIsNotAsExpected();
    public async Task SaveScrapbookOfExecutingFunctionFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        storedScrapbook = storedScrapbook with { ScrapbookJson = new Scrapbook() { State = "completed" }.ToJson()};
        await store.SaveScrapbookForExecutingFunction(
            functionId,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: 1,
            complimentaryState: new ComplimentaryState.SaveScrapbookForExecutingFunction(storedParameter, storedScrapbook, CrashedCheckFrequency: 0)
        ).ShouldBeFalseAsync();
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook<Scrapbook>(sf.Scrapbook.ScrapbookJson, sf.Scrapbook.ScrapbookType);
        scrapbook.State.ShouldBe("initial");
    }

    private class Scrapbook : RScrapbook
    {
        public string State { get; set; } = "";
    }
    
    public abstract Task DeletingExistingFunctionSucceeds();
    public async Task DeletingExistingFunctionSucceeds(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));
        
        await store.DeleteFunction(functionId, expectedEpoch: 0);
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldBeNull();
    }
    
    public abstract Task DeletingExistingFunctionFailsWhenEpochIsNotAsExpected();
    public async Task DeletingExistingFunctionFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));
        
        await store.DeleteFunction(functionId, expectedEpoch: 1).ShouldBeFalseAsync();
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook<Scrapbook>(sf.Scrapbook.ScrapbookJson, sf.Scrapbook.ScrapbookType);
        scrapbook.State.ShouldBe("initial");
    }

    public abstract Task FailFunctionSucceedsWhenEpochIsAsExpected();
    public async Task FailFunctionSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        var storedException = new StoredException(
            ExceptionMessage: "Something went wrong",
            ExceptionStackTrace: "StackTrace",
            ExceptionType: typeof(Exception).SimpleQualifiedName()
        );
        
        await store.FailFunction(
            functionId,
            storedException,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
        );
        
        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        sf.Status.ShouldBe(Status.Failed);
        sf.Exception.ShouldNotBeNull();
        var previouslyThrownException = DefaultSerializer.Instance.DeserializeException(sf.Exception);
        previouslyThrownException.ErrorMessage.ShouldBe(storedException.ExceptionMessage);
        previouslyThrownException.StackTrace.ShouldBe(storedException.ExceptionStackTrace);
        previouslyThrownException.ErrorType.ShouldBe(typeof(Exception));
    }
    
    public abstract Task SetFunctionStateSucceedsWhenEpochIsAsExpected();
    public async Task SetFunctionStateSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter,
            storedScrapbook,
            new StoredResult("completed".ToJson(), typeof(string).SimpleQualifiedName()),
            storedException: null,
            postponeUntil: null,
            events: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(1);
        sf.Status.ShouldBe(Status.Succeeded);
        sf.Exception.ShouldBeNull();
    }
    
    public abstract Task SetFunctionStateSucceedsWithEventsWhenEpochIsAsExpected();
    public async Task SetFunctionStateSucceedsWithEventsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var eventStore = store.EventStore;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        var event1 = new StoredEvent(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_1"
        );
        var event2 = new StoredEvent(
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_2"
        );
        await eventStore.AppendEvents(
            functionId,
            new[] { event1, event2 }
        );
        
        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter,
            storedScrapbook,
            new StoredResult("completed".ToJson(), typeof(string).SimpleQualifiedName()),
            storedException: null,
            postponeUntil: null,
            events: new ReplaceEvents(
                Events: new[] {
                    new StoredEvent(
                        "hello everyone".ToJson(),
                        EventType: typeof(string).SimpleQualifiedName(),
                        IdempotencyKey: "idempotency_key_1"
                    ),
                }, 
                ExistingCount: 2
            ),
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(1);
        sf.Status.ShouldBe(Status.Succeeded);
        sf.Exception.ShouldBeNull();

        var events = await TaskLinq.ToListAsync(store.EventStore.GetEvents(functionId));
        events.Count.ShouldBe(1);
        var deserializedEvent = (string) DefaultSerializer.Instance.DeserializeEvent(events[0].EventJson, events[0].EventType);
        deserializedEvent.ShouldBe("hello everyone");
    }
    
    public abstract Task ExecutingFunctionCanBeSuspendedSuccessfully();
    public async Task ExecutingFunctionCanBeSuspendedSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        await store.SuspendFunction(
            functionId,
            suspendUntilEventSourceCountAtLeast: 1,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
        ).ShouldBeTrueAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        sf.Status.ShouldBe(Status.Suspended);
        sf.Scrapbook.ScrapbookType.ShouldBe(storedScrapbook.ScrapbookType);
        sf.Scrapbook.ScrapbookJson.ShouldBe(storedScrapbook.ScrapbookJson);
        sf.Parameter.ParamType.ShouldBe(storedParameter.ParamType);
        sf.Parameter.ParamJson.ShouldBe(storedParameter.ParamJson);

        var events = await store.EventStore.GetEvents(functionId);
        events.ShouldBeEmpty();

        await store.IsFunctionSuspendedAndEligibleForReInvocation(functionId).ShouldBeNullAsync();

        await Task.Delay(500);
        
        await store.GetEligibleSuspendedFunctions(functionId.TypeId)
            .SelectAsync(sfs => sfs.Any())
            .ShouldBeFalseAsync();

        await store.EventStore.AppendEvents(
            functionId,
            storedEvents: new[] { new StoredEvent("hello world".ToJson(), EventType: typeof(string).SimpleQualifiedName()) }
        );

        var epoch = await store.IsFunctionSuspendedAndEligibleForReInvocation(functionId);
        epoch.ShouldNotBeNull();
        epoch.Value.ShouldBe(0);

        await BusyWait.Until(() => store.GetEligibleSuspendedFunctions(functionId.TypeId).Any());
        
        var eligibleFunctions = await store.GetEligibleSuspendedFunctions(functionId.TypeId).ToListAsync();
        eligibleFunctions.Count.ShouldBe(1);
        eligibleFunctions[0].InstanceId.ShouldBe(functionId.InstanceId);
    }
    
    public abstract Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch();
    public async Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
        ).ShouldBeTrueAsync();

        await store.RestartExecution(functionId, expectedEpoch: 0, crashedCheckFrequency: 1_000).ShouldBeTrueAsync();
        await store.RestartExecution(functionId, expectedEpoch: 0, crashedCheckFrequency: 1_000).ShouldBeFalseAsync();
    }
}