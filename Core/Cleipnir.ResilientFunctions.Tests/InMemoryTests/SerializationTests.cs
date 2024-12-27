using System;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Messaging;
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
    
    public record Parent;
    public record Child(string Value) : Parent;
}