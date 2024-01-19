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

        var leaseExpiration = DateTime.UtcNow.Ticks;
        var timestamp = leaseExpiration + 1;
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            leaseExpiration,
            postponeUntil: null,
            timestamp
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => 
            store.GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks).SelectAsync(efs => efs.Any())
        );
        
        var nonCompletes = await store.GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks).ToListAsync();
            
        nonCompletes.Count.ShouldBe(1);
        var nonCompleted = nonCompletes[0];
        nonCompleted.InstanceId.ShouldBe(functionId.InstanceId);
        nonCompleted.Epoch.ShouldBe(0);
        nonCompleted.LeaseExpiration.ShouldBe(leaseExpiration);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.FunctionId.ShouldBe(functionId);
        storedFunction.Parameter.ParamJson.ShouldBe(paramJson);
        storedFunction.Parameter.ParamType.ShouldBe(paramType);
        storedFunction.Scrapbook.ShouldNotBeNull();
        storedFunction.Scrapbook.ScrapbookType.ShouldBe(typeof(RScrapbook).SimpleQualifiedName());
        storedFunction.Epoch.ShouldBe(0);
        storedFunction.LeaseExpiration.ShouldBe(leaseExpiration);
        storedFunction.Timestamp.ShouldBe(timestamp);
        storedFunction.PostponedUntil.ShouldBeNull();

        const string result = "hello world";
        var resultJson = result.ToJson();
        var resultType = result.GetType().SimpleQualifiedName();
        await store.SucceedFunction(
            functionId,
            result: new StoredResult(resultJson, resultType),
            scrapbookJson: new RScrapbook().ToJson(),
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook),
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
            
        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Result.ShouldNotBeNull();
        storedFunction.Result.Deserialize<object>(DefaultSerializer.Instance).ShouldBe(result);
    }

    public abstract Task LeaseIsUpdatedWhenAsExpected();
    protected async Task LeaseIsUpdatedWhenAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        await store.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store
            .RenewLease(functionId, expectedEpoch: 0, leaseExpiration: 1)
            .ShouldBeTrueAsync();

        await BusyWait.Until(() =>
            store
                .GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks)
                .SelectAsync(efs => efs.Any())
        );

        await BusyWait.Until(async () =>
        {
            var nonCompletedFunctions = await store
                .GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks)
                .ToListAsync();
            if (!nonCompletedFunctions.Any()) return false;
            
            var nonCompletedFunction = nonCompletedFunctions.Single();
            return nonCompletedFunction is { Epoch: 0, LeaseExpiration: 1 };
        });
    }

    public abstract Task LeaseIsNotUpdatedWhenNotAsExpected();
    protected async Task LeaseIsNotUpdatedWhenNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.RenewLease(
            functionId,  
            expectedEpoch: 1,
            leaseExpiration: 1
        ).ShouldBeFalseAsync();

        await BusyWait.Until(() =>
            store
                .GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks)
                .SelectAsync(efs => efs.Any())
        );
        
        var nonCompletedFunctions = 
            await store.GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks);
        
        var nonCompletedFunction = nonCompletedFunctions.Single();
        nonCompletedFunction.Epoch.ShouldBe(0);
        nonCompletedFunction.LeaseExpiration.ShouldBe(leaseExpiration);
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store
            .RestartExecution(
                functionId,
                expectedEpoch: 0,
                leaseExpiration
            ).ShouldNotBeNullAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(1);
        storedFunction.LeaseExpiration.ShouldBe(leaseExpiration);
    }
        
    public abstract Task BecomeLeaderFailsWhenEpochIsNotAsExpected();
    protected async Task BecomeLeaderFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
        
        await store
            .RestartExecution(
                functionId,
                expectedEpoch: 1,
                leaseExpiration: DateTime.UtcNow.Ticks
            ).ShouldBeNullAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(0);
        storedFunction.LeaseExpiration.ShouldBe(leaseExpiration);
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            storedScrapbook.ScrapbookJson,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store
            .GetPostponedFunctions(functionId.TypeId, isEligibleBefore: nowTicks + 100)
            .SelectAsync(pfs => pfs.Any())
        );
        
        var postponedFunctions = await store.GetPostponedFunctions(
            functionId.TypeId,
            isEligibleBefore: nowTicks - 100
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            storedScrapbook.ScrapbookJson,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store
            .GetPostponedFunctions(functionId.TypeId, nowTicks + 100)
            .SelectAsync(pfs => pfs.Any())
        );
        
        var postponedFunctions = await store.GetPostponedFunctions(
            functionId.TypeId,
            isEligibleBefore: nowTicks + 100
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            storedScrapbook.ScrapbookJson,
            timestamp: DateTime.UtcNow.Ticks,
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
        var leaseExpiration = DateTime.UtcNow.Ticks;
        
        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );

        await BusyWait.Until(() => store.GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks).Any());
        
        var storedFunctions = await store.GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks).ToListAsync();
        storedFunctions.Count.ShouldBe(1);
        var sf = storedFunctions[0];
        sf.LeaseExpiration.ShouldBe(leaseExpiration);
    }
    
    public abstract Task OnlyEligibleCrashedFunctionsAreReturnedFromStore();
    protected async Task OnlyEligibleCrashedFunctionsAreReturnedFromStore(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var function1Id = TestFunctionId.Create();
        var function2Id = new FunctionId(function1Id.TypeId, functionInstanceId: Guid.NewGuid().ToString("N"));

        await store.CreateFunction(
            function1Id,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        
        await store.CreateFunction(
            function2Id,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration: 2,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );

        await BusyWait.Until(() => store.GetCrashedFunctions(function1Id.TypeId, leaseExpiresBefore: 1).Any());
        
        var storedFunctions = await store.GetCrashedFunctions(function1Id.TypeId, leaseExpiresBefore: 1).ToListAsync();
        storedFunctions.Count.ShouldBe(1);
        var sf = storedFunctions[0];
        sf.InstanceId.ShouldBe(function1Id.InstanceId);
        sf.LeaseExpiration.ShouldBe(0);
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.RestartExecution(functionId, expectedEpoch: 0, DateTime.UtcNow.Ticks).ShouldNotBeNullAsync();

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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.RestartExecution(functionId, expectedEpoch: 1, leaseExpiration: DateTime.UtcNow.Ticks).ShouldBeNullAsync();
        
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        storedScrapbook = storedScrapbook with { ScrapbookJson = new Scrapbook() { State = "completed" }.ToJson()};
        await store.SaveScrapbookForExecutingFunction(
            functionId,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState.SaveScrapbookForExecutingFunction(storedParameter, storedScrapbook, LeaseLength: 0)
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        storedScrapbook = storedScrapbook with { ScrapbookJson = new Scrapbook() { State = "completed" }.ToJson()};
        await store.SaveScrapbookForExecutingFunction(
            functionId,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: 1,
            complimentaryState: new ComplimentaryState.SaveScrapbookForExecutingFunction(storedParameter, storedScrapbook, LeaseLength: 0)
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
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
            timestamp: DateTime.UtcNow.Ticks,
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter,
            storedScrapbook,
            new StoredResult("completed".ToJson(), typeof(string).SimpleQualifiedName()),
            storedException: null,
            postponeUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(1);
        sf.Status.ShouldBe(Status.Succeeded);
        sf.Exception.ShouldBeNull();
    }
    
    public abstract Task SetFunctionStateSucceedsWithMessagesWhenEpochIsAsExpected();
    public async Task SetFunctionStateSucceedsWithMessagesWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var messages = store.MessageStore;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var message1 = new StoredMessage(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_1"
        );
        var message2 = new StoredMessage(
            "hello universe".ToJson(),
            typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_2"
        );
        await messages.AppendMessages(
            functionId,
            new[] { message1, message2 }
        );

        await messages.Replace(
            functionId,
            new[]
            {
                new StoredMessage(
                    "hello everyone".ToJson(),
                    MessageType: typeof(string).SimpleQualifiedName(),
                    IdempotencyKey: "idempotency_key_1"
                ),
            },
            expectedMessageCount: 2
        ).ShouldBeTrueAsync();
        
        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter,
            storedScrapbook,
            new StoredResult("completed".ToJson(), typeof(string).SimpleQualifiedName()),
            storedException: null,
            postponeUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(1);
        sf.Status.ShouldBe(Status.Succeeded);
        sf.Exception.ShouldBeNull();

        var storedMessages = await TaskLinq.ToListAsync(store.MessageStore.GetMessages(functionId));
        storedMessages.Count.ShouldBe(1);
        var deserializedMessage = (string) DefaultSerializer.Instance.DeserializeMessage(storedMessages[0].MessageJson, storedMessages[0].MessageType);
        deserializedMessage.ShouldBe("hello everyone");
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SuspendFunction(
            functionId,
            expectedMessageCount: 0,
            storedScrapbook.ScrapbookJson,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
        ).ShouldBeAsync(true);

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        (sf.Epoch is 0).ShouldBeTrue();
        sf.Status.ShouldBe(Status.Suspended);
        sf.Scrapbook.ScrapbookType.ShouldBe(storedScrapbook.ScrapbookType);
        sf.Scrapbook.ScrapbookJson.ShouldBe(storedScrapbook.ScrapbookJson);
        sf.Parameter.ParamType.ShouldBe(storedParameter.ParamType);
        sf.Parameter.ParamJson.ShouldBe(storedParameter.ParamJson);

        var messages = await store.MessageStore.GetMessages(functionId);
        messages.ShouldBeEmpty();

        await Task.Delay(500);

        var functionStatus = await store.MessageStore.AppendMessages(
            functionId,
            storedMessages: new[] { new StoredMessage("hello world".ToJson(), MessageType: typeof(string).SimpleQualifiedName()) }
        );
        functionStatus.Status.ShouldBe(Status.Suspended);
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
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.RestartExecution(
            functionId, 
            expectedEpoch: 0, 
            leaseExpiration: DateTime.UtcNow.Ticks
        ).ShouldNotBeNullAsync();
        await store.RestartExecution(
            functionId, 
            expectedEpoch: 0, 
            leaseExpiration: DateTime.UtcNow.Ticks
        ).ShouldBeNullAsync();
    }
    
    public abstract Task MessagesCanBeFetchedAfterFunctionWithInitialMessagesHasBeenCreated();
    public async Task MessagesCanBeFetchedAfterFunctionWithInitialMessagesHasBeenCreated(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.MessageStore.AppendMessages(
            functionId,
            new[]
            {
                new StoredMessage("Hello".ToJson(), MessageType: typeof(string).SimpleQualifiedName()),
                new StoredMessage("World".ToJson(), MessageType: typeof(string).SimpleQualifiedName())
            }
        );
        var messages = await store.MessageStore.GetMessages(functionId).ToListAsync();
        messages.Count.ShouldBe(2);
        messages[0].DefaultDeserialize().ShouldBe("Hello");
        messages[1].DefaultDeserialize().ShouldBe("World");
    }
    
    public abstract Task FunctionStatusAndEpochCanBeSuccessfullyFetched();
    public async Task FunctionStatusAndEpochCanBeSuccessfullyFetched(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new Scrapbook { State = "initial" }.ToJson(), typeof(Scrapbook).SimpleQualifiedName());
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter,
            storedScrapbook,
            new StoredResult("completed".ToJson(), typeof(string).SimpleQualifiedName()),
            storedException: null,
            postponeUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var (status, epoch) = await store.GetFunctionStatus(functionId).ShouldNotBeNullAsync();
        status.ShouldBe(Status.Succeeded);
        epoch.ShouldBe(1);
    }
    
    public abstract Task EpochIsNotIncrementedOnCompletion();
    protected async Task EpochIsNotIncrementedOnCompletion(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            storedScrapbook: Test.SimpleStoredScrapbook,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SucceedFunction(
            functionId,
            StoredResult.Null,
            Test.SimpleStoredScrapbook.ScrapbookJson,
            DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(Test.SimpleStoredParameter, Test.SimpleStoredScrapbook)
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(0);
    }
    
    public abstract Task EpochIsNotIncrementedOnPostponed();
    protected async Task EpochIsNotIncrementedOnPostponed(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            storedScrapbook: Test.SimpleStoredScrapbook,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: DateTime.UtcNow.Ticks,
            Test.SimpleStoredScrapbook.ScrapbookJson,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(Test.SimpleStoredParameter, Test.SimpleStoredScrapbook)
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(0);
    }
    
    public abstract Task EpochIsNotIncrementedOnFailure();
    protected async Task EpochIsNotIncrementedOnFailure(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            storedScrapbook: Test.SimpleStoredScrapbook,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.FailFunction(
            functionId,
            new StoredException("ExceptionMessage", ExceptionStackTrace: null, typeof(Exception).SimpleQualifiedName()),
            Test.SimpleStoredScrapbook.ScrapbookJson,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(Test.SimpleStoredParameter, Test.SimpleStoredScrapbook)
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(0);
    }
    
    public abstract Task EpochIsNotIncrementedOnSuspension();
    protected async Task EpochIsNotIncrementedOnSuspension(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            storedScrapbook: Test.SimpleStoredScrapbook,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SuspendFunction(
            functionId,
            expectedMessageCount: 0,
            Test.SimpleStoredScrapbook.ScrapbookJson,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(Test.SimpleStoredParameter, Test.SimpleStoredScrapbook)
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        (storedFunction.Epoch is 0).ShouldBeTrue();
    }
    
    public abstract Task FunctionIsPostponedOnSuspensionAndMessageCountMismatch();
    protected async Task FunctionIsPostponedOnSuspensionAndMessageCountMismatch(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            storedScrapbook: Test.SimpleStoredScrapbook,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.MessageStore.AppendMessage(functionId, messageJson: "hello world".ToJson(), messageType: typeof(string).SimpleQualifiedName());
        
        var success = await store.SuspendFunction(
            functionId,
            expectedMessageCount: 0,
            Test.SimpleStoredScrapbook.ScrapbookJson,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(Test.SimpleStoredParameter, Test.SimpleStoredScrapbook)
        );
        
        success.ShouldBeTrue();
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        (storedFunction.Epoch is 0).ShouldBeTrue();
        storedFunction.Status.ShouldBe(Status.Postponed);
        storedFunction.PostponedUntil.ShouldBe(0);
    }
}