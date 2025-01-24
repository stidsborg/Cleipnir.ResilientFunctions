using System;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
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
        var deserialized = serializer.DeserializeMessage(serialized.Content, serialized.Type);
        if (deserialized is not Child child)
            throw new Exception("Expected event to be of child-type");
                
        child.Value.ShouldBe("Hello World");
    }

    [TestMethod]
    public void OptionCanBeDeserializedByDefaultSerializer()
    {
        var option = Option.Create("some value");
        var serializer = DefaultSerializer.Instance;
        var serialized = serializer.SerializeMessage(option);
        var deserialized = serializer.DeserializeMessage(serialized.Content, serialized.Type);
        deserialized.ShouldBe(option);
    }
    
    [TestMethod]
    public void OptionWithNoValueCanBeDeserializedByDefaultSerializer()
    {
        var option = Option<string>.NoValue;
        var serializer = DefaultSerializer.Instance;
        var serialized = serializer.SerializeMessage(option);
        var deserialized = serializer.DeserializeMessage(serialized.Content, serialized.Type);
        deserialized.ShouldBe(option);
    }
    
    [TestMethod]
    public void ExceptionCanBeConvertedToAndFromFatalWorkflowException()
    {
        var serializer = DefaultSerializer.Instance;
        FatalWorkflowException fatalWorkflowException = null!;
        var flowId = TestFlowId.Create();
        try
        {
            throw new InvalidOperationException("Something went wrong");
        }
        catch (InvalidOperationException e)
        {
            fatalWorkflowException = FatalWorkflowException.Create(flowId, e);
        }
        
        fatalWorkflowException.ErrorType.ShouldBe(typeof(InvalidOperationException));
        fatalWorkflowException.FlowErrorMessage.ShouldBe("Something went wrong");
        fatalWorkflowException.FlowStackTrace.ShouldNotBeNull();

        var storedException = serializer.SerializeException(fatalWorkflowException);
        var deserializedException = serializer.DeserializeException(flowId, storedException);

        (deserializedException is FatalWorkflowException<InvalidOperationException>).ShouldBeTrue();
        deserializedException.ErrorType.ShouldBe(typeof(InvalidOperationException));
        deserializedException.FlowErrorMessage.ShouldBe("Something went wrong");
        deserializedException.FlowStackTrace.ShouldNotBeNull();
    }
    
    public record Parent;
    public record Child(string Value) : Parent;
}