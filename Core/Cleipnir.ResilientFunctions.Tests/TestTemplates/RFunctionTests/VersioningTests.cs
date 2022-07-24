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
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                CrashedCheckFrequency: TimeSpan.FromMilliseconds(10), 
                UnhandledExceptionHandler: unhandledExceptionCatcher.Catch
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
            scrapbookType: null,
            TimeSpan.FromMilliseconds(10).Ticks,
            version: 0
        ).ShouldBeTrueAsync();

        _ = rFunctions.RegisterFunc(
            nameof(NonExistingParameterTypeResultsInFailedFunction),
            string(PersonV1 p) => p.Name
        );
        
        await Task.Delay(250);
        unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
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
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                CrashedCheckFrequency: TimeSpan.FromMilliseconds(10), 
                UnhandledExceptionHandler: unhandledExceptionCatcher.Catch
            )
        );
        var functionId = new FunctionId(nameof(WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown), "v1");
        await store.CreateFunction(
           functionId,
            new StoredParameter(
                new PersonV1(Name: "Peter").ToJson(),
                typeof(PersonV1).SimpleQualifiedName()
            ),
           scrapbookType: null,
           TimeSpan.FromMilliseconds(10).Ticks,
           version: 0   
        ).ShouldBeTrueAsync();

        var flag = new SyncedFlag();
        _ = rFunctions.RegisterAction(
            nameof(WhenInputParameterOfRegisteredFunctionIsIncompatibleWithDeserializedTypeAnExceptionIsThrown),
            void (int _) => flag.Raise()
        );

        await BusyWait.UntilAsync(() => unhandledExceptionCatcher.ThrownExceptions.Count > 0);

        flag.IsRaised.ShouldBeFalse();
        await store
            .GetFunction(functionId)
            .Map(sf => sf!.Status == Status.Failed)
            .ShouldBeTrueAsync();
        
        unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBe(1);
    }

    public abstract Task RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype();
    protected async Task RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                CrashedCheckFrequency: TimeSpan.FromMilliseconds(10), 
                UnhandledExceptionHandler: unhandledExceptionCatcher.Catch
            )
        );
        
        await store.CreateFunction(
            new FunctionId(
                nameof(RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype), 
                "v1"
            ),
            new StoredParameter(
                new PersonV1(Name: "Peter").ToJson(),
                typeof(PersonV1).SimpleQualifiedName()
            ),
            scrapbookType: null,
            TimeSpan.FromMilliseconds(10).Ticks,
            version: 0
        ).ShouldBeTrueAsync();
        await store.CreateFunction(
            new FunctionId(
                nameof(RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype), 
                "v2"
            ),
            new StoredParameter(
                new PersonV2(Name: "Ole", Age: 35).ToJson(),
                typeof(PersonV2).SimpleQualifiedName()
            ),
            scrapbookType: null,
            TimeSpan.FromMilliseconds(10).Ticks,
            version: 0
        ).ShouldBeTrueAsync();

        var invocations = new SyncedList<Person>();
        rFunctions.RegisterAction(
            nameof(RegisteredFunctionAcceptsTwoDifferentParameterTypesOfSameSubtype),
            void (Person p) => invocations.Add(p)
        );

        await BusyWait.UntilAsync(() => invocations.Count == 2);
        invocations.Any(p => p is PersonV1).ShouldBeTrue();
        invocations.Any(p => p is PersonV2).ShouldBeTrue();

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    private record Person;
    private record PersonV1(string Name) : Person;
    private record PersonV2(string Name, int Age) : Person;
}