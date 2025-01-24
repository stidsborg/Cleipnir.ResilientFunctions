using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public interface ISerializer
{
    byte[] Serialize<T>(T value);
    T Deserialize<T>(byte[] bytes);
    
    StoredException SerializeException(FatalWorkflowException fatalWorkflowException);
    FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException);
    
    SerializedMessage SerializeMessage<TMessage>(TMessage message) where TMessage : notnull;
    object DeserializeMessage(byte[] json, byte[] type);
    
}