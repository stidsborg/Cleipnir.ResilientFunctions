using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public interface ISerializer
{
    byte[] SerializeParameter<TParam>(TParam parameter);
    TParam DeserializeParameter<TParam>(byte[] json); 
    StoredException SerializeException(FatalWorkflowException fatalWorkflowException);
    FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException);
    byte[] SerializeResult<TResult>(TResult result);
    TResult DeserializeResult<TResult>(byte[] json);
    SerializedMessage SerializeMessage<TMessage>(TMessage message) where TMessage : notnull;
    object DeserializeMessage(byte[] json, byte[] type);
    byte[] SerializeEffectResult<TResult>(TResult result);
    TResult DeserializeEffectResult<TResult>(byte[] json);
    byte[] SerializeState<TState>(TState state) where TState : FlowState, new();
    TState DeserializeState<TState>(byte[] json) where TState : FlowState, new();
}