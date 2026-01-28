using System;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
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
        var content = serializer.Serialize(@event, @event.GetType());
        var type = serializer.SerializeType(@event.GetType());
        var serialized = new SerializedMessage(content, type);
        var deserialized = serializer.Deserialize(serialized.Content, serializer.ResolveType(serialized.Type)!);
        if (deserialized is not Child child)
            throw new Exception("Expected event to be of child-type");

        child.Value.ShouldBe("Hello World");
    }
    
    [TestMethod]
    public void StringCanBeSerializedAndDeserializedUsingValueTypeSerializationOverload()
    {
        var serializer = DefaultSerializer.Instance;
        var serializedBytes = serializer.Serialize("Hello World", typeof(string));
        var deserialized = (string)serializer.Deserialize(serializedBytes, typeof(string));
        deserialized.ShouldBe("Hello World");
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

        var storedException = fatalWorkflowException.ToStoredException();
        var deserializedException = FatalWorkflowException.Create(flowId, storedException);

        (deserializedException is FatalWorkflowException<InvalidOperationException>).ShouldBeTrue();
        deserializedException.ErrorType.ShouldBe(typeof(InvalidOperationException));
        deserializedException.FlowErrorMessage.ShouldBe("Something went wrong");
        deserializedException.FlowStackTrace.ShouldNotBeNull();
    }
    
    public record Parent;
    public record Child(string Value) : Parent;
    
    [TestMethod]
    public void ImplementingClassCanOverrideResolveTypeDefaultMethod()
    {
        ISerializer defaultSerializer = DefaultSerializer.Instance;
        ISerializer customSerializer = new CustomResolveTypeSerializer();

        // Default implementation uses Type.GetType
        defaultSerializer.ResolveType(typeof(string).SimpleQualifiedName().ToUtf8Bytes()).ShouldBe(typeof(string));

        // Custom implementation always returns typeof(int) regardless of input
        customSerializer.ResolveType(typeof(string).SimpleQualifiedName().ToUtf8Bytes()).ShouldBe(typeof(int));
        customSerializer.ResolveType("anything".ToUtf8Bytes()).ShouldBe(typeof(int));
    }

    private class CustomResolveTypeSerializer : ISerializer
    {
        public byte[] Serialize(object value, Type type)
            => throw new NotImplementedException();

        public object Deserialize(byte[] bytes, Type type)
            => throw new NotImplementedException();

        // Override the default interface method
        public Type? ResolveType(byte[] type) => typeof(int);
    }
}