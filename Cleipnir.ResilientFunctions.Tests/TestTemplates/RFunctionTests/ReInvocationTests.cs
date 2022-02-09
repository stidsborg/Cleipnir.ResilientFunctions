namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class ReInvocationTests
{
    //todo refactor tests after registration type has been introduced
    /*
    public abstract Task FailedRActionCanBeReInvoked();
    protected async Task FailedRActionCanBeReInvoked(IFunctionStore store)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var functionId = new FunctionId("functionType", "functionInstance");
        var parameter = "functionInstance";
        await store.CreateFunction(
            functionId,
            new StoredParameter(parameter.ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: null,
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson: null,
            result: null,
            failed: new StoredFailure(new Exception("").ToJson(), typeof(Exception).SimpleQualifiedName()),
            postponedUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var rFunctions = RFunctions.Create(store, unhandledExceptionCatcher.Catch);

        var rAction = rFunctions.Register(
            functionId.TypeId,
            (string _) => 
                RResult.Success.ToTask(),
            _ => _
        );

        await rAction(parameter, reInvoke: true);

        await store.GetFunction(functionId).Map(sf => sf?.Status).ShouldBeAsync(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task FailedRActionWithScrapbookCanBeReInvoked();
    protected async Task FailedRActionWithScrapbookCanBeReInvoked(IFunctionStore store)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var functionId = new FunctionId("functionType", "functionInstance");
        var parameter = "functionInstance";
        var initialScrapbook = new ListScrapbook<int> {List = new List<int> { 1,2,3,4 }};
        await store.CreateFunction(
            functionId,
            new StoredParameter(parameter.ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: typeof(ListScrapbook<int>).SimpleQualifiedName(),
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson: initialScrapbook.ToJson(),
            result: null,
            failed: new StoredFailure(new Exception("").ToJson(), typeof(Exception).SimpleQualifiedName()),
            postponedUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var rFunctions = RFunctions.Create(store, unhandledExceptionCatcher.Catch);

        var rAction = rFunctions.Register(
            functionId.TypeId,
            (string _, ListScrapbook<int> scrapbook) =>
            {
                scrapbook.List.ShouldBe(initialScrapbook.List);
                return RResult.Success.ToTask();
            },
            _ => _
        );

        await rAction(parameter, reInvoke: true);

        await store.GetFunction(functionId).Map(sf => sf?.Status).ShouldBeAsync(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task FailedRFuncCanBeReInvoked();
    protected async Task FailedRFuncCanBeReInvoked(IFunctionStore store)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var functionId = new FunctionId("functionType", "functionInstance");
        var parameter = "functionInstance";
        await store.CreateFunction(
            functionId,
            new StoredParameter(parameter.ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: null,
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson: null,
            result: null,
            failed: new StoredFailure(new Exception("").ToJson(), typeof(Exception).SimpleQualifiedName()),
            postponedUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var rFunctions = RFunctions.Create(store, unhandledExceptionCatcher.Catch);

        var rAction = rFunctions.Register(
            functionId.TypeId,
            (string s) => s.ToSucceededRResult().ToTask(),
            _ => _
        );

        await rAction(parameter, reInvoke: true);

        var storedFunction = await store.GetFunction(functionId).ShouldNotBeNullAsync();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldNotBeNull();
        var result = (string) JsonConvert.DeserializeObject(
            storedFunction.Result.ResultJson,
            Type.GetType(storedFunction.Result.ResultType, throwOnError: true)!
        )!;
        result.ShouldBe(parameter);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task FailedRFuncWithScrapbookCanBeReInvoked();
    protected async Task FailedRFuncWithScrapbookCanBeReInvoked(IFunctionStore store)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var functionId = new FunctionId("functionType", "functionInstance");
        var parameter = "functionInstance";
        var initialScrapbook = new ListScrapbook<int> {List = new List<int> { 1,2,3,4 }};
        
        await store.CreateFunction(
            functionId,
            new StoredParameter(parameter.ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: typeof(ListScrapbook<int>).SimpleQualifiedName(),
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson: initialScrapbook.ToJson(),
            result: null,
            failed: new StoredFailure(new Exception("").ToJson(), typeof(Exception).SimpleQualifiedName()),
            postponedUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var rFunctions = RFunctions.Create(store, unhandledExceptionCatcher.Catch);

        var rAction = rFunctions.Register(
            functionId.TypeId,
            (string s, ListScrapbook<int> scrapbook) =>
            {
                scrapbook.List.ShouldBe(initialScrapbook.List);
                return s.ToSucceededRResult().ToTask();
            },
            _ => _
        );

        await rAction(parameter, reInvoke: true);

        var storedFunction = await store.GetFunction(functionId).ShouldNotBeNullAsync();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldNotBeNull();
        var result = (string) JsonConvert.DeserializeObject(
            storedFunction.Result.ResultJson,
            Type.GetType(storedFunction.Result.ResultType, throwOnError: true)!
        )!;
        result.ShouldBe(parameter);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }*/
}