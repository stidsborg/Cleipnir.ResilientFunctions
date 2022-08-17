using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class CrashedMethodTests
{
    private class Entity {}
    public abstract Task NonCompletedFuncIsCompletedByWatchDog();
    protected async Task NonCompletedFuncIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(NonCompletedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        CrashedCheckFrequency: TimeSpan.Zero, 
                        PostponedCheckFrequency: TimeSpan.Zero,
                        EntityFactory: new EntityFuncFactory(_ => new Entity())
                    )
                )
                .RegisterMethod<Entity>()
                .RegisterFunc<string, string>(
                    functionTypeId,
                    entity => _ => NeverCompletingTask.OfType<string>()
                ).Invoke;

            _ = nonCompletingRFunctions(param, param);
        }
        {
            var constructedEntity = new Entity();
            var inputParameterEntity = new Synced<Entity>();
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                    EntityFactory: new EntityFuncFactory(_ => constructedEntity)
                )
            );

            var rFunc = rFunctions
                .RegisterMethod<Entity>()
                .RegisterFunc<string, string>(
                    functionTypeId,
                    entity => s =>
                    {
                        inputParameterEntity.Value = entity;
                        return s.ToUpper().ToTask();
                    }).Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
            );
            
            await rFunc(param, param).ShouldBeAsync("TEST");
            inputParameterEntity.Value.ShouldBe(constructedEntity);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }

    public abstract Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog();
    protected async Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(NonCompletedFuncWithScrapbookIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        CrashedCheckFrequency: TimeSpan.Zero, 
                        PostponedCheckFrequency: TimeSpan.Zero,
                        EntityFactory: new EntityFuncFactory(_ => new Entity())
                    )
                )
                .RegisterMethod<Entity>()
                .RegisterFunc<string, Scrapbook, string>(
                    functionTypeId,
                    entity => (_, _) => NeverCompletingTask.OfType<Result<string>>()
                ).Invoke;

            _ = nonCompletingRFunctions(param, param);
        }
        {
            var constructedEntity = new Entity();
            var inputParameterEntity = new Synced<Entity>();
            
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                    EntityFactory: new EntityFuncFactory(_ => constructedEntity)
                )
            );
            
            var rFunc = rFunctions
                .RegisterMethod<Entity>()
                .RegisterFunc<string, Scrapbook, string>(
                    functionTypeId,
                    entity => async (s, scrapbook) =>
                    {
                        inputParameterEntity.Value = entity;
                        scrapbook.Value = 1;
                        await scrapbook.Save();
                        return s.ToUpper();
                    }
                ).Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            await rFunc(param, param).ShouldBeAsync("TEST");
            inputParameterEntity.Value.ShouldBe(constructedEntity);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    public abstract Task NonCompletedActionIsCompletedByWatchDog();
    protected async Task NonCompletedActionIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(NonCompletedActionIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        CrashedCheckFrequency: TimeSpan.Zero, 
                        PostponedCheckFrequency: TimeSpan.Zero,
                        EntityFactory: new EntityFuncFactory(_ => new Entity())
                    )
                )
                .RegisterMethod<Entity>()
                .RegisterAction<string>(
                    functionTypeId,
                    entity => _ => NeverCompletingTask.OfVoidType
                )
                .Invoke;

            _ = nonCompletingRFunctions(param, param);
        }
        {
            var constructedEntity = new Entity();
            var inputParameterEntity = new Synced<Entity>();
            
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                    EntityFactory: new EntityFuncFactory(_ => constructedEntity)
                )
            );

            var rAction = rFunctions
                .RegisterMethod<Entity>()
                .RegisterAction<string>(
                    functionTypeId,
                    entity => _ =>
                    {
                        inputParameterEntity.Value = constructedEntity;
                        return Task.CompletedTask;
                    })
                .Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                () => store
                    .GetFunction(functionId)
                    .Map(sf => sf?.Status == Status.Succeeded)
            );
            
            await rAction(param, param);
            inputParameterEntity.Value.ShouldBe(constructedEntity);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }

    public abstract Task NonCompletedActionWithScrapbookIsCompletedByWatchDog();
    protected async Task NonCompletedActionWithScrapbookIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(NonCompletedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        CrashedCheckFrequency: TimeSpan.Zero, 
                        PostponedCheckFrequency: TimeSpan.Zero,
                        EntityFactory: new EntityFuncFactory(_ => new Entity())
                    )
                )
                .RegisterMethod<Entity>()
                .RegisterAction<string, Scrapbook>(
                    functionTypeId,
                    entity => (_, _) => NeverCompletingTask.OfVoidType
                ).Invoke;

            _ = nonCompletingRFunctions(param, param);
        }
        {
            var constructedEntity = new Entity();
            var inputParameterEntity = new Synced<Entity>();
            
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.FromMilliseconds(2),
                    EntityFactory: new EntityFuncFactory(_ => constructedEntity)
                )
            );

            var rAction = rFunctions
                .RegisterMethod<Entity>()
                .RegisterAction<string, Scrapbook>(
                    functionTypeId,
                    entity => async (_, scrapbook) =>
                    {
                        inputParameterEntity.Value = entity;
                        scrapbook.Value = 1;
                        await scrapbook.Save();
                    }
                ).Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                () => store.GetFunction(functionId).Map(f => f?.Status == Status.Succeeded)
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            await rAction(param, param);
            inputParameterEntity.Value.ShouldBe(constructedEntity);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }
}