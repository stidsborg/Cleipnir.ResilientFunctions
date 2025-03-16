using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public interface ISerializer
{
    byte[] Serialize<T>(T value);
    byte[] Serialize(object? value, Type type);
    T Deserialize<T>(byte[] bytes);
    
    StoredException SerializeException(FatalWorkflowException fatalWorkflowException);
    FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException);

    SerializedMessage SerializeMessage(object message, Type messageType);
    object DeserializeMessage(byte[] json, byte[] type);
    
}