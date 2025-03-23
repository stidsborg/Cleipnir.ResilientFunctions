using System;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
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
        var serialized = serializer.SerializeMessage(@event, @event.GetType());
        var deserialized = serializer.DeserializeMessage(serialized.Content, serialized.Type);
        if (deserialized is not Child child)
            throw new Exception("Expected event to be of child-type");
                
        child.Value.ShouldBe("Hello World");
    }
    
    [TestMethod]
    public void StringCanBeSerializedAndDeserializedUsingValueTypeSerializationOverload()
    {
        var serializer = DefaultSerializer.Instance;
        var serializedBytes = serializer.Serialize("Hello World", typeof(string));
        var deserialized = serializer.Deserialize<string>(serializedBytes);
        deserialized.ShouldBe("Hello World");
    }

    [TestMethod]
    public void OptionCanBeDeserializedByDefaultSerializer()
    {
        var option = Option.Create("some value");
        var serializer = new CustomSerializableDecorator(DefaultSerializer.Instance);
        var serialized = serializer.Serialize(option, option.GetType());
        var deserialized = serializer.Deserialize<Option<string>>(serialized);
        deserialized.ShouldBe(option);
    }
    
    [TestMethod]
    public void OptionWithNoValueCanBeDeserializedByDefaultSerializer()
    {
        var option = Option<string>.NoValue;
        var serializer = DefaultSerializer.Instance;
        var serialized = serializer.Serialize(option, option.GetType());
        var deserialized = serializer.Deserialize<Option<string>>(serialized);
        deserialized.ShouldBe(option);
    }
    
    //
    [TestMethod]
    public void OptionCanBeDeserializedAsMessageByDefaultSerializer()
    {
        var option = Option.Create("some value");
        var serializer = DefaultSerializer.Instance;
        var serialized = serializer.SerializeMessage(option, option.GetType());
        var deserialized = serializer.DeserializeMessage(serialized.Content, serialized.Type);
        deserialized.ShouldBe(option);
    }
    
    [TestMethod]
    public void OptionWithNoValueCanBeDeserializedAsMessageByDefaultSerializer()
    {
        var option = Option<string>.NoValue;
        var serializer = DefaultSerializer.Instance;
        var serialized = serializer.SerializeMessage(option, option.GetType());
        var deserialized = serializer.DeserializeMessage(serialized.Content, serialized.Type);
        deserialized.ShouldBe(option);
    }
    
    [TestMethod]
    public void EithersCanBeDeserializedByDefaultSerializer()
    {
        {
            var either = SerializeAndDeserialize(Either<string, int>.CreateFirst("First"));
            either.HasFirst.ShouldBeTrue();
            either.First.ShouldBe("First");
        }
        {
            var either = SerializeAndDeserialize(Either<string, int>.CreateSecond(2));
            either.HasSecond.ShouldBeTrue();
            either.Second.ShouldBe(2);
        }
        
        {
            var either = SerializeAndDeserialize(Either<string, int, Option<string>>.CreateFirst("First"));
            either.HasFirst.ShouldBeTrue();
            either.First.ShouldBe("First");
        }
        {
            var either = SerializeAndDeserialize(Either<string, int, Option<string>>.CreateSecond(2));
            either.HasSecond.ShouldBeTrue();
            either.Second.ShouldBe(2);
        }
        {
            var either = SerializeAndDeserialize(Either<string, int, Option<string>>.CreateThird(Option.Create("option")));
            either.HasThird.ShouldBeTrue();
            either.Third.ShouldBe(Option.Create<string>("option"));
        }
        
        {
            var either = SerializeAndDeserialize(EitherOrNone<string, int>.CreateFirst("First"));
            either.HasFirst.ShouldBeTrue();
            either.First.ShouldBe("First");
        }
        {
            var either = SerializeAndDeserialize(EitherOrNone<string, int>.CreateSecond(2));
            either.HasSecond.ShouldBeTrue();
            either.Second.ShouldBe(2);
        }
        {
            var either = SerializeAndDeserialize(EitherOrNone<string, int>.CreateNone());
            either.HasNone.ShouldBeTrue();
        }
        
        {
            var either = SerializeAndDeserialize(EitherOrNone<string, int, Option<string>>.CreateFirst("First"));
            either.HasFirst.ShouldBeTrue();
            either.First.ShouldBe("First");
        }
        {
            var either = SerializeAndDeserialize(EitherOrNone<string, int, Option<string>>.CreateSecond(2));
            either.HasSecond.ShouldBeTrue();
            either.Second.ShouldBe(2);
        }
        {
            var either = SerializeAndDeserialize(EitherOrNone<string, int, Option<string>>.CreateThird(Option.Create("option")));
            either.HasThird.ShouldBeTrue();
            either.Third.ShouldBe(Option.Create<string>("option"));
        }
        {
            var either = SerializeAndDeserialize(EitherOrNone<string, int, Option<string>>.CreateNone());
            either.HasNone.ShouldBeTrue();
        }
    }

    private T SerializeAndDeserialize<T>(T value)
    {
        var serializer = new CustomSerializableDecorator(DefaultSerializer.Instance);
        var bytes = serializer.Serialize(value);
        return serializer.Deserialize<T>(bytes);
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