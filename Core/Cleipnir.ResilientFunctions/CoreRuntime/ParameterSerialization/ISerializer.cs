using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public interface ISerializer
{
    string SerializeParameter<TParam>(TParam parameter);
    TParam DeserializeParameter<TParam>(string json); 
    StoredException SerializeException(Exception exception);
    PreviouslyThrownException DeserializeException(StoredException storedException);
    string SerializeResult<TResult>(TResult result);
    TResult DeserializeResult<TResult>(string json);
    JsonAndType SerializeMessage<TMessage>(TMessage message) where TMessage : notnull;
    object DeserializeMessage(string json, string type);
    string SerializeEffectResult<TResult>(TResult result);
    TResult DeserializeEffectResult<TResult>(string json);
    string SerializeState<TState>(TState state) where TState : WorkflowState, new();
    TState DeserializeState<TState>(string json) where TState : WorkflowState, new();
}