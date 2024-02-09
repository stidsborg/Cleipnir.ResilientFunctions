using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class VersioningTests
{
    public abstract Task NonExistingParameterTypeResultsInFailedFunction();
    protected async Task NonExistingParameterTypeResultsInFailedFunction(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                leaseLength: TimeSpan.FromMilliseconds(250), 
                unhandledExceptionHandler: unhandledExceptionCatcher.Catch
            )
        );
        var functionId = new FunctionId(
            nameof(NonExistingParameterTypeResultsInFailedFunction),
            "instance"
        );

        await store.CreateFunction(
            functionId,
            new StoredParameter(
                new PersonV1(Name: "Peter").ToJson(),
                typeof(PersonV1).SimpleQualifiedName().Replace("V1", "V0")
            ),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        _ = functionsRegistry.RegisterFunc(
            nameof(NonExistingParameterTypeResultsInFailedFunction),
            string(PersonV1 p) => p.Name
        );

        await BusyWait.UntilAsync(() => unhandledExceptionCatcher.ThrownExceptions.Count > 0, maxWait: TimeSpan.FromSeconds(5));
        
        await store
            .GetFunction(functionId)
            .Map(sf => sf!.Status == Status.Failed)
            .ShouldBeTrueAsync();
    }

    public abstract Task WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown();
    protected async Task WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = new InMemoryFunctionStore();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                leaseLength: TimeSpan.FromMilliseconds(250), 
                unhandledExceptionHandler: unhandledExceptionCatcher.Catch
            )
        );
        var functionId = new FunctionId(nameof(WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown), "v1");
        await store.CreateFunction(
           functionId,
            new StoredParameter(
                new PersonV1(Name: "Peter").ToJson(),
                typeof(PersonV1).SimpleQualifiedName()
            ),
           new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
           leaseExpiration: DateTime.UtcNow.Ticks,
           postponeUntil: null,
           timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var flag = new SyncedFlag();
        _ = functionsRegistry.RegisterAction(
            nameof(WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown),
            void (int _) => flag.Raise()
        );

        await BusyWait.UntilAsync(() => unhandledExceptionCatcher.ThrownExceptions.Count > 0, maxWait: TimeSpan.FromSeconds(5));

        flag.IsRaised.ShouldBeFalse();
        await store
            .GetFunction(functionId)
            .Map(sf => sf!.Status == Status.Failed)
            .ShouldBeTrueAsync();
        
        unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
    }
    
    public abstract Task WhenScrapbookOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown();
    protected async Task WhenScrapbookOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = new InMemoryFunctionStore();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                leaseLength: TimeSpan.FromMilliseconds(10), 
                unhandledExceptionHandler: unhandledExceptionCatcher.Catch
            )
        );
        var functionId = new FunctionId(nameof(WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown), "v1");
        await store.CreateFunction(
            functionId,
            new StoredParameter(
                "Hello World".ToJson(),
                typeof(string).SimpleQualifiedName()
            ),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var flag = new SyncedFlag();
        _ = functionsRegistry.RegisterAction(
            nameof(WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown),
            void (string param, Scrapbook2 scrapbook) => flag.Raise()
        );

        await BusyWait.UntilAsync(() => unhandledExceptionCatcher.ThrownExceptions.Count > 0, maxWait: TimeSpan.FromSeconds(5));

        flag.IsRaised.ShouldBeFalse();
        await store
            .GetFunction(functionId)
            .Map(sf => sf!.Status == Status.Failed)
            .ShouldBeTrueAsync();
        
        unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
    }
    private class Scrapbook1 : RScrapbook {}
    private class Scrapbook2 : RScrapbook {}

    public abstract Task RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype();
    protected async Task RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                leaseLength: TimeSpan.FromMilliseconds(250), 
                unhandledExceptionHandler: unhandledExceptionCatcher.Catch
            )
        );

        var v1FunctionId = TestFunctionId.Create();
        await store.CreateFunction(
            v1FunctionId,
            new StoredParameter(
                new PersonV1(Name: "Peter").ToJson(),
                typeof(PersonV1).SimpleQualifiedName()
            ),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
        var testId = TestFunctionId.Create();
        var v2FunctionId = new FunctionId(v1FunctionId.TypeId, testId.InstanceId);
        await store.CreateFunction(
            v2FunctionId,
            new StoredParameter(
                new PersonV2(Name: "Ole", Age: 35).ToJson(),
                typeof(PersonV2).SimpleQualifiedName()
            ),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var invocations = new SyncedList<Person>();
        functionsRegistry.RegisterAction(
            functionTypeId: v1FunctionId.TypeId,
            void (Person p) => invocations.Add(p)
        );
        
        await BusyWait.UntilAsync(() => invocations.Count == 2, maxWait: TimeSpan.FromSeconds(5));
        invocations.Any(p => p is PersonV1).ShouldBeTrue();
        invocations.Any(p => p is PersonV2).ShouldBeTrue();

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ReturnTypeCanBeParentTypeOfActualReturnedValue();
    protected async Task ReturnTypeCanBeParentTypeOfActualReturnedValue(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                leaseLength: TimeSpan.FromMilliseconds(10), 
                unhandledExceptionHandler: unhandledExceptionCatcher.Catch
            )
        );
        var functionId = new FunctionId(
            nameof(ReturnTypeCanBeParentTypeOfActualReturnedValue),
            "instance"
        );

        var rFunc = functionsRegistry.RegisterFunc<string, object>(
            functionId.TypeId.Value,
            param => param
        ).Invoke;

        var returned = await rFunc("instance","hello world");
        (returned is string).ShouldBeTrue();
        returned.ToString().ShouldBe("hello world");

        returned = await rFunc("instance", "");
        (returned is string).ShouldBeTrue();
        returned.ToString().ShouldBe("hello world");
    }
    
    private record Person;
    private record PersonV1(string Name) : Person;
    private record PersonV2(string Name, int Age) : Person;
}