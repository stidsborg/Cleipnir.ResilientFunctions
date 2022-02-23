using System;
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

public abstract class UnhandledFuncExceptionTests
{
    public abstract Task UnhandledExceptionResultsInPostponedFunc();
    protected async Task UnhandledExceptionResultsInPostponedFunc(Task<IFunctionStore> storeTask)
    {
        var functionType = "SomeFunctionType".ToFunctionTypeId();
        var store = await storeTask;
        var rFunctions = new RFunctions(store);
        var syncedException = new Synced<Exception>();
        var rFunc = rFunctions.Register<string, string>(
            functionType,
            _ => throw new Exception("oh no"),
            _ => _,
            onException: (exception, _, _) =>
            {
                syncedException.Value = exception;
                return Postpone.Until(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
            }
        );

        //invoke
        var invokedResult = await rFunc.Invoke("1");
        invokedResult.Postponed.ShouldBeTrue();
        invokedResult.PostponedUntil!.Value.ShouldBe(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
        var sf = await store.GetFunction(new FunctionId(functionType, "1")).ShouldNotBeNullAsync();
        sf.PostponedUntil.ShouldNotBeNull();

        //schedule
        await rFunc.Schedule("2");
        await BusyWait.Until(
            () => store
                .GetFunction(new FunctionId(functionType, "2"))
                .Map(f => f?.PostponedUntil != null)
        );
        
        //re-invoke
        invokedResult = await rFunc.ReInvoke("1", new[] {Status.Postponed});
        invokedResult.Postponed.ShouldBeTrue();
        invokedResult.PostponedUntil!.Value.ShouldBe(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
        sf = await store.GetFunction(new FunctionId(functionType, "1")).ShouldNotBeNullAsync();
        sf.PostponedUntil.ShouldNotBeNull();
    }

    public abstract Task UnhandledExceptionResultsInPostponedFuncWithScrapbook();
    protected async Task UnhandledExceptionResultsInPostponedFuncWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var functionType = "SomeFunctionType".ToFunctionTypeId();
        var store = await storeTask;
        var rFunctions = new RFunctions(store);
        var syncedException = new Synced<Exception>();
        var rFunc = rFunctions.Register<string, ListScrapbook<string>, string>(
            functionType,
            (_, _) => throw new Exception("oh no"),
            _ => _,
            onException: (exception, scrapbook, _, _) =>
            {
                syncedException.Value = exception;
                scrapbook.List.Add("onException");
                return Postpone.Until(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
            }
        );

        //invoke
        var invokedResult = await rFunc.Invoke("1");
        invokedResult.Postponed.ShouldBeTrue();
        invokedResult.PostponedUntil!.Value.ShouldBe(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
        var sf = await store.GetFunction(new FunctionId(functionType, "1")).ShouldNotBeNullAsync();
        sf.PostponedUntil.ShouldNotBeNull();
        sf.Scrapbook!
            .Deserialize(DefaultSerializer.Instance)
            .CastTo<ListScrapbook<string>>()
            .List
            .Single()
            .ShouldBe("onException");

        //schedule
        await rFunc.Schedule("2");
        await BusyWait.Until(
            () => store
                .GetFunction(new FunctionId(functionType, "2"))
                .Map(f => f?.PostponedUntil != null)
        );
        sf = await store.GetFunction(new FunctionId(functionType, "1")).ShouldNotBeNullAsync();
        sf.Scrapbook!
            .Deserialize(DefaultSerializer.Instance)
            .CastTo<ListScrapbook<string>>()
            .List
            .Single()
            .ShouldBe("onException");
        
        //re-invoke
        invokedResult = await rFunc.ReInvoke("1", new[] {Status.Postponed});
        invokedResult.Postponed.ShouldBeTrue();
        invokedResult.PostponedUntil!.Value.ShouldBe(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
        sf = await store.GetFunction(new FunctionId(functionType, "1")).ShouldNotBeNullAsync();
        sf.PostponedUntil.ShouldNotBeNull();
        sf.Scrapbook!
            .Deserialize(DefaultSerializer.Instance)
            .CastTo<ListScrapbook<string>>()
            .List
            .Count
            .ShouldBe(2);
    }

    public abstract Task UnhandledExceptionResultsInPostponedAction();
    protected async Task UnhandledExceptionResultsInPostponedAction(Task<IFunctionStore> storeTask)
    {
        var functionType = "SomeFunctionType".ToFunctionTypeId();
        var store = await storeTask;
        var rFunctions = new RFunctions(store);
        var syncedException = new Synced<Exception>();
        var rFunc = rFunctions.Register<string>(
            functionType,
            _ => throw new Exception("oh no"),
            _ => _,
            onException: (exception, _, _) =>
            {
                syncedException.Value = exception;
                return Postpone.Until(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
            }
        );

        //invoke
        var invokedResult = await rFunc.Invoke("1");
        invokedResult.Postponed.ShouldBeTrue();
        invokedResult.PostponedUntil!.Value.ShouldBe(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
        var sf = await store.GetFunction(new FunctionId(functionType, "1")).ShouldNotBeNullAsync();
        sf.PostponedUntil.ShouldNotBeNull();

        //schedule
        await rFunc.Schedule("2");
        await BusyWait.Until(
            () => store
                .GetFunction(new FunctionId(functionType, "2"))
                .Map(f => f?.PostponedUntil != null)
        );
        
        //re-invoke
        invokedResult = await rFunc.ReInvoke("1", new[] {Status.Postponed});
        invokedResult.Postponed.ShouldBeTrue();
        invokedResult.PostponedUntil!.Value.ShouldBe(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
        sf = await store.GetFunction(new FunctionId(functionType, "1")).ShouldNotBeNullAsync();
        sf.PostponedUntil.ShouldNotBeNull();
    }

    public abstract Task UnhandledExceptionResultsInPostponedActionWithScrapbook();
    protected async Task UnhandledExceptionResultsInPostponedActionWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var functionType = "SomeFunctionType".ToFunctionTypeId();
        var store = await storeTask;
        var rFunctions = new RFunctions(store);
        var syncedException = new Synced<Exception>();
        var rFunc = rFunctions.Register<string, ListScrapbook<string>>(
            functionType,
            (_, _) => throw new Exception("oh no"),
            _ => _,
            onException: (exception, scrapbook, _, _) =>
            {
                syncedException.Value = exception;
                scrapbook.List.Add("onException");
                return Postpone.Until(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
            }
        );

        //invoke
        var invokedResult = await rFunc.Invoke("1");
        invokedResult.Postponed.ShouldBeTrue();
        invokedResult.PostponedUntil!.Value.ShouldBe(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
        var sf = await store.GetFunction(new FunctionId(functionType, "1")).ShouldNotBeNullAsync();
        sf.PostponedUntil.ShouldNotBeNull();
        sf.Scrapbook!
            .Deserialize(DefaultSerializer.Instance)
            .CastTo<ListScrapbook<string>>()
            .List
            .Single()
            .ShouldBe("onException");

        //schedule
        await rFunc.Schedule("2");
        await BusyWait.Until(
            () => store
                .GetFunction(new FunctionId(functionType, "2"))
                .Map(f => f?.PostponedUntil != null)
        );
        sf = await store.GetFunction(new FunctionId(functionType, "1")).ShouldNotBeNullAsync();
        sf.Scrapbook!
            .Deserialize(DefaultSerializer.Instance)
            .CastTo<ListScrapbook<string>>()
            .List
            .Single()
            .ShouldBe("onException");
        
        //re-invoke
        invokedResult = await rFunc.ReInvoke("1", new[] {Status.Postponed});
        invokedResult.Postponed.ShouldBeTrue();
        invokedResult.PostponedUntil!.Value.ShouldBe(new DateTime(3000,1,1, 0, 0, 0, DateTimeKind.Utc));
        sf = await store.GetFunction(new FunctionId(functionType, "1")).ShouldNotBeNullAsync();
        sf.PostponedUntil.ShouldNotBeNull();
        sf.Scrapbook!
            .Deserialize(DefaultSerializer.Instance)
            .CastTo<ListScrapbook<string>>()
            .List
            .Count
            .ShouldBe(2);
    }
}