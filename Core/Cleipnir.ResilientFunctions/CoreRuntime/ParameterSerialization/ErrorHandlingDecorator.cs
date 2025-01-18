using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public class ErrorHandlingDecorator : ISerializer
{
    private readonly ISerializer _inner;

    public ErrorHandlingDecorator(ISerializer inner) => _inner = inner;
    
    public byte[] SerializeParameter<TParam>(TParam parameter) 
        => _inner.SerializeParameter(parameter);
    public TParam DeserializeParameter<TParam>(byte[] json) 
    {
        try
        {
            return _inner.DeserializeParameter<TParam>(json)
                   ?? throw new DeserializationException(
                       $"Deserialized parameter was null for type '{typeof(TParam).SimpleQualifiedName()}' and json: '{Convert.ToBase64String(json)}'", 
                       new NullReferenceException()
                   );
        }
        catch (DeserializationException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new DeserializationException(
                $"Unable to deserialize parameter with type: '{typeof(TParam).SimpleQualifiedName()}' and json: '{Convert.ToBase64String(json)}'", 
                e
            );
        }
    }
    public StoredException SerializeException(FatalWorkflowException exception)
        => _inner.SerializeException(exception);

    public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException)
    {
        try
        {
            return _inner.DeserializeException(flowId, storedException);
        }
        catch (DeserializationException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new DeserializationException(
                $"Unable to deserialize exception with type: '{storedException.ExceptionType}'", 
                e
            );
        }
    }

    public byte[] SerializeResult<TResult>(TResult result) 
        => _inner.SerializeResult(result);
    public TResult DeserializeResult<TResult>(byte[] json) 
    {
        try
        {
            return _inner.DeserializeResult<TResult>(json)
                   ?? throw new DeserializationException(
                       $"Deserialized result was null with type: '{typeof(TResult).SimpleQualifiedName()}' and json: '{Convert.ToBase64String(json)}'", 
                       new NullReferenceException()
                   );
        }
        catch (DeserializationException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new DeserializationException(
                $"Unable to deserialize result with type: '{typeof(TResult).SimpleQualifiedName()}' and json: '{Convert.ToBase64String(json)}'", 
                e
            );
        }
    }

    public SerializedMessage SerializeMessage<TEvent>(TEvent message) where TEvent : notnull
        => _inner.SerializeMessage(message);
    public object DeserializeMessage(byte[] json, byte[] type)
    {
        try
        {
            return _inner.DeserializeMessage(json, type)
                   ?? throw new DeserializationException(
                       $"Deserialized event was null with type: '{type}' and json: '{Convert.ToBase64String(json)}'", 
                       new NullReferenceException()
                   );
        }
        catch (DeserializationException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new DeserializationException(
                $"Unable to deserialize event with type: '{type}' and json: '{Convert.ToBase64String(json)}'", 
                e
            );
        }
    }

    public byte[] SerializeEffectResult<TResult>(TResult result)
        => _inner.SerializeEffectResult(result);
    public TResult DeserializeEffectResult<TResult>(byte[] json)
    {
        try
        {
            return _inner.DeserializeEffectResult<TResult>(json)
                   ?? throw new DeserializationException(
                       $"Deserialized Effect's result was null with type: '{typeof(TResult)}' and json: '{Convert.ToBase64String(json)}'", 
                       new NullReferenceException()
                   );
        }
        catch (DeserializationException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new DeserializationException(
                $"Unable to deserialize effect to type: '{typeof(TResult)}' and bytes: '{Convert.ToBase64String(json)}'",  
                e
            );
        }
    }

    public byte[] SerializeState<TState>(TState state) where TState : FlowState, new()
        => _inner.SerializeState(state);
    public TState DeserializeState<TState>(byte[] json) where TState : FlowState, new()
    {
        try
        {
            return _inner.DeserializeState<TState>(json)
                   ?? throw new DeserializationException(
                       $"Deserialized state was null with type: '{typeof(TState)}' and json: '{Convert.ToBase64String(json)}'", 
                       new NullReferenceException()
                   );
        }
        catch (DeserializationException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new DeserializationException(
                $"Unable to deserialize state with type: '{typeof(TState)}' and json: '{Convert.ToBase64String(json)}'",  
                e
            );
        }
    }
}