using System;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
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
        var deserialized = serializer.DeserializeMessage(serialized.Json, serialized.Type);
        if (deserialized is not Child child)
            throw new Exception("Expected event to be of child-type");
                
        child.Value.ShouldBe("Hello World");
    }
    
    public record Parent;
    public record Child(string Value) : Parent;

    public class FlowState : Domain.FlowState
    {
        public string Value { get; set; } = "";
    }

    private class PersonPrev
    {
        public string? Name { get; set; }
    }

    private class PersonCurr
    {
        public string? Name { get; set; }
    }
}