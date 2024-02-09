using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class SunshineTests
{
    private readonly DefaultSerializer _serializer = DefaultSerializer.Instance;
        
    public abstract Task SunshineScenarioFunc();
    public async Task SunshineScenarioFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioFunc).ToFunctionTypeId();
        async Task<string> ToUpper(string s)
        {
            await Task.Delay(10);
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var rFunc = functionsRegistry
            .RegisterFunc(
                functionTypeId,
                (string s) => ToUpper(s)
            ).Invoke;

        var result = await rFunc("hello", "hello");
        result.ShouldBe("HELLO");
            
        var storedFunction = await store.GetFunction(
            new FunctionId(
                functionTypeId, 
                "hello".ToFunctionInstanceId()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldNotBeNull();
        var storedResult = storedFunction.Result.Deserialize<string>(_serializer);
        storedResult.ShouldBe("HELLO");
            
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
        
    public abstract Task SunshineScenarioFuncWithScrapbook();
    public async Task SunshineScenarioFuncWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioFuncWithScrapbook).ToFunctionTypeId();
        async Task<string> ToUpper(string s, Scrapbook scrapbook)
        {
            await scrapbook.Save();
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var rFunc = functionsRegistry
            .RegisterFunc(
                functionTypeId,
                (string s, Scrapbook scrapbook) => ToUpper(s, scrapbook)
            )
            .Invoke;

        var result = await rFunc("hello", "hello");
        result.ShouldBe("HELLO");
            
        var storedFunction = await store.GetFunction(
            new FunctionId(
                functionTypeId, 
                "hello".ToFunctionInstanceId()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldNotBeNull();
        var storedResult = storedFunction.Result.Deserialize<string>(_serializer);
        storedResult.ShouldBe("HELLO");
            
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
        
    public abstract Task SunshineScenarioAction();
    public async Task SunshineScenarioAction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioAction).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var rAction = functionsRegistry
            .RegisterAction(
                functionTypeId,
                (string _) => Task.Delay(10)
            )
            .Invoke;

        await rAction("hello", "hello");
        
        var storedFunction = await store.GetFunction(
            new FunctionId(
                functionTypeId, 
                "hello".ToFunctionInstanceId()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
        
    public abstract Task SunshineScenarioActionWithScrapbook();
    public async Task SunshineScenarioActionWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioActionWithScrapbook).ToFunctionTypeId();

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var rFunc = functionsRegistry
            .RegisterAction(
                functionTypeId,
                (string s, Scrapbook scrapbook) => scrapbook.Save()
            ).Invoke;

        await rFunc("hello", "hello");

        var storedFunction = await store.GetFunction(
            new FunctionId(
                functionTypeId, 
                "hello".ToFunctionInstanceId()
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task SunshineScenarioNullReturningFunc();
    protected async Task SunshineScenarioNullReturningFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FunctionTypeId functionTypeId = "SomeFunctionType";
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            (string s) => default(string).ToTask()
        ).Invoke;

        var result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();
    }

    public abstract Task SunshineScenarioNullReturningFuncWithScrapbook();
    protected async Task SunshineScenarioNullReturningFuncWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            (string _, ListScrapbook<string> scrapbook) =>
            {
                scrapbook.List.Add("hello world");
                return default(string).ToTask();
            }
        ).Invoke;

        var result = await rFunc(functionInstanceId.Value, "hello world");
        result.ShouldBeNull();

        var storedFunction = await store
            .GetFunction(functionId)
            .ShouldNotBeNullAsync();

        var scrapbook = storedFunction.Scrapbook.ScrapbookJson.DeserializeFromJsonTo<ListScrapbook<string>>();
        scrapbook.List.Single().ShouldBe("hello world");
    }
    
    public abstract Task SecondInvocationOnNullReturningFuncReturnsNullSuccessfully();
    protected async Task SecondInvocationOnNullReturningFuncReturnsNullSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FunctionTypeId functionTypeId = "SomeFunctionType";
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            functionTypeId,
            (string _, ListScrapbook<string> scrapbook) =>
            {
                scrapbook.List.Add("hello world");
                return default(string).ToTask();
            }
        ).Invoke;

        var result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();

        result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();
    }
    
    public abstract Task InvocationModeShouldBeDirectInSunshineScenario();
    protected async Task InvocationModeShouldBeDirectInSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FunctionTypeId functionTypeId = "SomeFunctionType";
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var syncedInvocationMode = new Synced<InvocationMode>();
        var rFunc = functionsRegistry.RegisterAction(
            functionTypeId,
            (string _) => syncedInvocationMode.Value = InvocationMode.Direct
        ).Invoke;

        await rFunc("hello world", "hello world");
        
        syncedInvocationMode.Value.ShouldBe(InvocationMode.Direct);
    }

    private class Scrapbook : RScrapbook {}
}