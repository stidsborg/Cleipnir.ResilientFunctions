using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class SunshineEntityMethodTests
{
    private readonly DefaultSerializer _serializer = DefaultSerializer.Instance;
    private class Entity {}

    public abstract Task SunshineScenarioFunc();
    public async Task SunshineScenarioFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioFunc).ToFunctionTypeId();
        var constructedEntity = new Entity();
        var parameterEntity = new Synced<Entity>();
        
        async Task<string> ToUpper(Entity entity, string s)
        {
            parameterEntity.Value = entity;
            await Task.Delay(10);
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                DependencyResolver: new FuncDependencyResolver(_ => constructedEntity)
            )
        );

        var rFunc = rFunctions
            .RegisterMethod<Entity>()
            .RegisterFunc<string, string>(
                functionTypeId,
                entity => s => ToUpper(entity, s)
            ).Invoke;

        var result = await rFunc("hello", "hello");
        result.ShouldBe("HELLO");
        parameterEntity.Value.ShouldBe(constructedEntity);

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
        var inputParameterEntity = new Synced<Entity>();
        var constructedEntity = new Entity();
        
        async Task<string> ToUpper(Entity entity, string s, Scrapbook scrapbook)
        {
            inputParameterEntity.Value = entity;
            await scrapbook.Save();
            return s.ToUpper();
        }
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler.Catch, DependencyResolver: new FuncDependencyResolver(_ => constructedEntity))
        );
        
        var rFunc = rFunctions
            .RegisterMethod<Entity>()
            .RegisterFunc<string, Scrapbook, string>(
                functionTypeId,
                entity => (s, scrapbook) => ToUpper(entity, s, scrapbook)
            )
            .Invoke;

        var result = await rFunc("hello", "hello");
        result.ShouldBe("HELLO");
        inputParameterEntity.Value.ShouldBe(constructedEntity);
            
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
        var inputParameterEntity = new Synced<Entity>();
        var constructedEntity = new Entity();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var rFunctions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler.Catch, DependencyResolver: new FuncDependencyResolver(_ => constructedEntity))
        );
        var rAction = rFunctions
            .RegisterMethod<Entity>()
            .RegisterAction<string>(
                functionTypeId,
                entity => _ =>
                {
                    inputParameterEntity.Value = entity;
                    return Task.Delay(10);
                })
            .Invoke;

        await rAction("hello", "hello");
        inputParameterEntity.Value.ShouldBe(constructedEntity);
        
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
        var inputParameterEntity = new Synced<Entity>();
        var constructedEntity = new Entity();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var rFunctions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler.Catch, DependencyResolver: new FuncDependencyResolver(_ => constructedEntity))
        );
        var rFunc = rFunctions
            .RegisterMethod<Entity>()
            .RegisterAction<string, Scrapbook>(
                functionTypeId,
                entity => async (_, scrapbook) =>
                {
                    inputParameterEntity.Value = entity;
                    await scrapbook.Save();
                }).Invoke;

        await rFunc("hello", "hello");
        inputParameterEntity.Value.ShouldBe(constructedEntity);

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
    
    public abstract Task SunshineScenarioScheduleFunc();
    public async Task SunshineScenarioScheduleFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioScheduleFunc).ToFunctionTypeId();
        var constructedEntity = new Entity();
        var parameterEntity = new Synced<Entity>();
        
        async Task<string> ToUpper(Entity entity, string s)
        {
            parameterEntity.Value = entity;
            await Task.Delay(10);
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                DependencyResolver: new FuncDependencyResolver(_ => constructedEntity)
            )
        );

        var rFunc = rFunctions
            .RegisterMethod<Entity>()
            .RegisterFunc<string, string>(
                functionTypeId,
                entity => s => ToUpper(entity, s)
            ).Schedule;

        await rFunc("hello", "hello");
        await BusyWait.Until(() =>
            store.GetFunction(new FunctionId(functionTypeId, "hello".ToFunctionInstanceId())).Map(sf => sf?.Status == Status.Succeeded)
        );
        parameterEntity.Value.ShouldBe(constructedEntity);

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

    public abstract Task SunshineScenarioScheduleFuncWithScrapbook();
    public async Task SunshineScenarioScheduleFuncWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioScheduleFuncWithScrapbook).ToFunctionTypeId();
        var inputParameterEntity = new Synced<Entity>();
        var constructedEntity = new Entity();
        
        async Task<string> ToUpper(Entity entity, string s, Scrapbook scrapbook)
        {
            inputParameterEntity.Value = entity;
            await scrapbook.Save();
            return s.ToUpper();
        }
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler.Catch, DependencyResolver: new FuncDependencyResolver(_ => constructedEntity))
        );
        
        var rFunc = rFunctions
            .RegisterMethod<Entity>()
            .RegisterFunc<string, Scrapbook, string>(
                functionTypeId,
                entity => (s, scrapbook) => ToUpper(entity, s, scrapbook)
            )
            .Schedule;

        await rFunc("hello", "hello");
        await BusyWait.Until(() =>
            store.GetFunction(new FunctionId(functionTypeId, "hello".ToFunctionInstanceId())).Map(sf => sf?.Status == Status.Succeeded)
        );
        inputParameterEntity.Value.ShouldBe(constructedEntity);
            
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
        
    public abstract Task SunshineScenarioScheduleAction();
    public async Task SunshineScenarioScheduleAction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioScheduleAction).ToFunctionTypeId();
        var inputParameterEntity = new Synced<Entity>();
        var constructedEntity = new Entity();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var rFunctions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler.Catch, DependencyResolver: new FuncDependencyResolver(_ => constructedEntity))
        );
        var rAction = rFunctions
            .RegisterMethod<Entity>()
            .RegisterAction<string>(
                functionTypeId,
                entity => _ =>
                {
                    inputParameterEntity.Value = entity;
                    return Task.Delay(10);
                })
            .Schedule;

        await rAction("hello", "hello");
        await BusyWait.Until(() =>
            store.GetFunction(new FunctionId(functionTypeId, "hello".ToFunctionInstanceId())).Map(sf => sf?.Status == Status.Succeeded)
        );
        inputParameterEntity.Value.ShouldBe(constructedEntity);
        
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
        
    public abstract Task SunshineScenarioScheduleActionWithScrapbook();
    public async Task SunshineScenarioScheduleActionWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(SunshineScenarioScheduleActionWithScrapbook).ToFunctionTypeId();
        var inputParameterEntity = new Synced<Entity>();
        var constructedEntity = new Entity();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var rFunctions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler.Catch, DependencyResolver: new FuncDependencyResolver(_ => constructedEntity))
        );
        var rFunc = rFunctions
            .RegisterMethod<Entity>()
            .RegisterAction<string, Scrapbook>(
                functionTypeId,
                entity => async (_, scrapbook) =>
                {
                    inputParameterEntity.Value = entity;
                    await scrapbook.Save();
                }).Schedule;

        await rFunc("hello", "hello");
        await BusyWait.Until(() =>
            store.GetFunction(new FunctionId(functionTypeId, "hello".ToFunctionInstanceId())).Map(sf => sf?.Status == Status.Succeeded)
        );
        inputParameterEntity.Value.ShouldBe(constructedEntity);

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

    private class Scrapbook : RScrapbook {}
}