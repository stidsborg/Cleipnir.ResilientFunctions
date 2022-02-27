using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils.Scrapbooks;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class SunshineTests
{
    private readonly DefaultSerializer _serializer = DefaultSerializer.Instance;
        
    public abstract Task SunshineScenarioFunc();
    public async Task SunshineScenarioFunc(IFunctionStore store)
    {
        var functionTypeId = nameof(SunshineScenarioFunc).ToFunctionTypeId();
        async Task<Return<string>> ToUpper(string s)
        {
            await Task.Delay(10);
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var rFunctions = RFunctions.Create(store, unhandledExceptionHandler.Catch);

        var rFunc = rFunctions
            .Register(
                functionTypeId,
                (string s) => ToUpper(s)
            ).Invoke;

        var rResult = await rFunc("hello", "hello");
        var result = rResult.SuccessResult;
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
        var storedResult = storedFunction.Result.Deserialize(_serializer);
        storedResult.ShouldBe("HELLO");
            
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
        
    public abstract Task SunshineScenarioFuncWithScrapbook();
    public async Task SunshineScenarioFuncWithScrapbook(IFunctionStore store)
    {
        var functionTypeId = nameof(SunshineScenarioFuncWithScrapbook).ToFunctionTypeId();
        async Task<Return<string>> ToUpper(string s, Scrapbook scrapbook)
        {
            await scrapbook.Save();
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var rFunctions = RFunctions.Create(store, unhandledExceptionHandler.Catch);

        var rFunc = rFunctions
            .Register(
                functionTypeId,
                (string s, Scrapbook scrapbook) => ToUpper(s, scrapbook)
            ).Invoke;

        var rResult = await rFunc("hello", "hello");
        var result = rResult.SuccessResult;
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
        var storedResult = storedFunction.Result.Deserialize(_serializer);
        storedResult.ShouldBe("HELLO");
            
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
        
    public abstract Task SunshineScenarioAction();
    public async Task SunshineScenarioAction(IFunctionStore store)
    {
        var functionTypeId = nameof(SunshineScenarioAction).ToFunctionTypeId();
        async Task<Return> ToUpper(string _)
        {
            await Task.Delay(10);
            return Succeed.WithoutValue;
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var rFunctions = RFunctions.Create(store, unhandledExceptionHandler.Catch);

        var rAction = rFunctions
            .Register(
                functionTypeId,
                (string s) => ToUpper(s)
            ).Invoke;

        var rResult = await rAction("hello", "hello");
        rResult.Succeeded.ShouldBeTrue();

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
    public async Task SunshineScenarioActionWithScrapbook(IFunctionStore store)
    {
        var functionTypeId = nameof(SunshineScenarioActionWithScrapbook).ToFunctionTypeId();
        async Task<Return> ToUpper(string _, Scrapbook scrapbook)
        {
            await scrapbook.Save();
            return Succeed.WithoutValue;
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var rFunctions = RFunctions.Create(store, unhandledExceptionHandler.Catch);

        var rFunc = rFunctions
            .Register(
                functionTypeId,
                (string s, Scrapbook scrapbook) => ToUpper(s, scrapbook)
            ).Invoke;

        var rResult = await rFunc("hello", "hello");
        rResult.Succeeded.ShouldBeTrue();

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
    protected async Task SunshineScenarioNullReturningFunc(IFunctionStore store)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FunctionTypeId functionTypeId = "SomeFunctionType";
        var rFunctions = RFunctions.Create(
            store,
            unhandledExceptionCatcher.Catch
        );

        var rFunc = rFunctions.Register(
            functionTypeId,
            (string s) => Succeed.WithValue(default(string)).ToTask()
        ).Invoke;

        var result = await rFunc("hello world", "hello world").EnsureSuccess();
        result.ShouldBeNull();
    }

    public abstract Task SunshineScenarioNullReturningFuncWithScrapbook();
    protected async Task SunshineScenarioNullReturningFuncWithScrapbook(IFunctionStore store)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FunctionTypeId functionTypeId = "SomeFunctionType";
        var rFunctions = RFunctions.Create(
            store,
            unhandledExceptionCatcher.Catch
        );

        var rFunc = rFunctions.Register(
            functionTypeId,
            (string _, ListScrapbook<string> scrapbook) =>
            {
                scrapbook.List.Add("hello world");
                return Succeed.WithValue(default(string)).ToTask();
            }
        ).Invoke;

        var result = await rFunc("hello world", "hello world").EnsureSuccess();
        result.ShouldBeNull();

        var storedFunction = await store
            .GetFunction(new FunctionId(functionTypeId, "hello world"))
            .ShouldNotBeNullAsync();

        var scrapbook = storedFunction.Scrapbook!.ScrapbookJson!.DeserializeFromJsonTo<ListScrapbook<string>>();
        scrapbook.List.Single().ShouldBe("hello world");
    }

    private class Scrapbook : RScrapbook {}
}