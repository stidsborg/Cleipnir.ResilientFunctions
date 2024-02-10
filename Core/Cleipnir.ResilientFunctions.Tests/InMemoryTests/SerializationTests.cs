using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class SerializationTests
{
    [TestMethod]
    public void ConcreteTypeOfEventIsSerializedAndDeserializedByDefaultSerializer()
    {
        var serializer = DefaultSerializer.Instance;
        Parent @event = new Child("Hello World");
        var serialized = serializer.SerializeMessage(@event);
        var deserialized = serializer.DeserializeMessage(serialized.Json, serialized.Type);
        if (deserialized is not Child child)
            throw new Exception("Expected event to be of child-type");
                
        child.Value.ShouldBe("Hello World");
    }

    [TestMethod]
    public void ConcreteTypeOfParamIsSerializedAndDeserializedByDefaultSerializer()
    {
        var serializer = DefaultSerializer.Instance;
        Parent param = new Child("Hello World");
        var serialized = serializer.SerializeResult(param);
        var deserialized = serializer.DeserializeResult<Parent>(serialized.ResultJson!, serialized.ResultType!);
        if (deserialized is not Child child)
            throw new Exception("Expected event to be of child-type");
        
        child.Value.ShouldBe("Hello World");
    }
    
    [TestMethod]
    public void ConcreteTypeOfResultIsSerializedAndDeserializedByDefaultSerializer()
    {
        var serializer = DefaultSerializer.Instance;
        Parent param = new Child("Hello World");
        var serialized = serializer.SerializeParameter(param);
        var deserialized = serializer.DeserializeParameter<Parent>(serialized.ParamJson, serialized.ParamType);
        if (deserialized is not Child child)
            throw new Exception("Expected event to be of child-type");
        
        child.Value.ShouldBe("Hello World");
    }

    [TestMethod]
    public void ConcreteTypeOfStateIsSerializedAndDeserializedByDefaultSerializer()
    {
        var serializer = DefaultSerializer.Instance;
        Domain.WorkflowState workflowState = new WorkflowState { Value = "Hello World" };
        var serialized = serializer.SerializeState(workflowState);
        var deserializedEvent = serializer.DeserializeParameter<Domain.WorkflowState>(serialized.StateJson, serialized.StateType);
        if (deserializedEvent is not WorkflowState state)
            throw new Exception("Expected event to be of child-type");
        
        state.Value.ShouldBe("Hello World");
    }
    
    public record Parent;
    public record Child(string Value) : Parent;

    public class WorkflowState : Domain.WorkflowState
    {
        public string Value { get; set; } = "";
    }

    [TestMethod]
    public async Task TypeNameChangeScenarioIsHandledSuccessfullyByCustomSerializer()
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var prev = new PersonPrev { Name = "Peter" };
        var serializer = new Serializer();
        var flag = new SyncedFlag();
        var store = new InMemoryFunctionStore();
        await store.CreateFunction(
            new FunctionId("typeId", "instanceId"),
            new StoredParameter(prev.ToJson(), typeof(PersonPrev).SimpleQualifiedName()),
            new StoredState(new Domain.WorkflowState().ToJson(), typeof(Domain.WorkflowState).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
        
        using var functionsRegistry = new FunctionsRegistry(
            store, 
            new Settings(
                leaseLength: TimeSpan.FromMilliseconds(1),
                unhandledExceptionHandler: unhandledExceptionCatcher.Catch
            )
        );

        var personCurr = default(PersonCurr);
        functionsRegistry
            .RegisterAction(
                "typeId".ToFunctionTypeId(),
                Task (PersonCurr p) =>
                {
                    personCurr = p;
                    flag.Raise();
                    return Task.CompletedTask;
                },
                new Settings(serializer: serializer)
            );

        await flag.WaitForRaised();
        await BusyWait.UntilAsync(() => flag.IsRaised || unhandledExceptionCatcher.ThrownExceptions.Any());
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
        personCurr.ShouldNotBeNull();
        personCurr.Name.ShouldBe("Peter");
    }

    private class PersonPrev
    {
        public string? Name { get; set; }
    }

    private class PersonCurr
    {
        public string? Name { get; set; }
    }

    private class Serializer : ISerializer
    {
        private readonly DefaultSerializer _defaultSerializer = DefaultSerializer.Instance;
        
        public StoredParameter SerializeParameter<TParam>(TParam parameter) where TParam : notnull
            => _defaultSerializer.SerializeParameter(parameter);
        public TParam DeserializeParameter<TParam>(string json, string type) where TParam : notnull
        {
            if (type.Contains(nameof(PersonPrev)))
                return (TParam) (object) JsonConvert.DeserializeObject<PersonCurr>(json)!;

            return (TParam) JsonConvert.DeserializeObject(
                json,
                Type.GetType(type, throwOnError: true)!
            )!;
        }

        public StoredState SerializeState<TState>(TState state) where TState : Domain.WorkflowState
            => _defaultSerializer.SerializeState(state);
        public TState DeserializeState<TState>(string? json, string type) where TState : Domain.WorkflowState
            => _defaultSerializer.DeserializeState<TState>(json, type);

        public StoredException SerializeException(Exception exception)
            => _defaultSerializer.SerializeException(exception);
        public PreviouslyThrownException DeserializeException(StoredException storedException)
            => _defaultSerializer.DeserializeException(storedException);

        public StoredResult SerializeResult<TResult>(TResult result)
            => _defaultSerializer.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json, string type)
            => _defaultSerializer.DeserializeResult<TResult>(json, type);

        public JsonAndType SerializeMessage<TEvent>(TEvent @event) where TEvent : notnull
            => _defaultSerializer.SerializeMessage(@event);
        public object DeserializeMessage(string json, string type)
            => _defaultSerializer.DeserializeMessage(json, type);

        public string SerializeActivityResult<TResult>(TResult result)
            => _defaultSerializer.SerializeActivityResult(result);
        public TResult DeserializeActivityResult<TResult>(string json)
            => _defaultSerializer.DeserializeActivityResult<TResult>(json);
    }
}