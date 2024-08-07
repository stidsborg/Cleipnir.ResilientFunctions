﻿using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using static Cleipnir.ResilientFunctions.Helpers.Helpers;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public class ErrorHandlingDecorator : ISerializer
{
    private readonly ISerializer _inner;

    public ErrorHandlingDecorator(ISerializer inner) => _inner = inner;
    
    public string SerializeParameter<TParam>(TParam parameter) 
        => _inner.SerializeParameter(parameter);
    public TParam DeserializeParameter<TParam>(string json) 
    {
        try
        {
            return _inner.DeserializeParameter<TParam>(json)
                   ?? throw new DeserializationException(
                       $"Deserialized parameter was null for type '{typeof(TParam).SimpleQualifiedName()}' and json: '{MinifyJson(json)}'", 
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
                $"Unable to deserialize parameter with type: '{typeof(TParam).SimpleQualifiedName()}' and json: '{MinifyJson(json)}'", 
                e
            );
        }
    }
    public StoredException SerializeException(Exception exception)
        => _inner.SerializeException(exception);

    public PreviouslyThrownException DeserializeException(StoredException storedException)
    {
        try
        {
            return _inner.DeserializeException(storedException);
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

    public string SerializeResult<TResult>(TResult result) 
        => _inner.SerializeResult(result);
    public TResult DeserializeResult<TResult>(string json) 
    {
        try
        {
            return _inner.DeserializeResult<TResult>(json)
                   ?? throw new DeserializationException(
                       $"Deserialized result was null with type: '{typeof(TResult).SimpleQualifiedName()}' and json: '{MinifyJson(json)}'", 
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
                $"Unable to deserialize result with type: '{typeof(TResult).SimpleQualifiedName()}' and json: '{MinifyJson(json)}'", 
                e
            );
        }
    }

    public JsonAndType SerializeMessage<TEvent>(TEvent message) where TEvent : notnull
        => _inner.SerializeMessage(message);
    public object DeserializeMessage(string json, string type)
    {
        try
        {
            return _inner.DeserializeMessage(json, type)
                   ?? throw new DeserializationException(
                       $"Deserialized event was null with type: '{type}' and json: '{MinifyJson(json)}'", 
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
                $"Unable to deserialize event with type: '{type}' and json: '{MinifyJson(json)}'", 
                e
            );
        }
    }

    public string SerializeEffectResult<TResult>(TResult result)
        => _inner.SerializeEffectResult(result);
    public TResult DeserializeEffectResult<TResult>(string json)
    {
        try
        {
            return _inner.DeserializeEffectResult<TResult>(json)
                   ?? throw new DeserializationException(
                       $"Deserialized Effect's result was null with type: '{typeof(TResult)}' and json: '{MinifyJson(json)}'", 
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
                $"Unable to deserialize effect to type: '{typeof(TResult)}' and json: '{MinifyJson(json)}'",  
                e
            );
        }
    }

    public string SerializeState<TState>(TState state) where TState : FlowState, new()
        => _inner.SerializeState(state);
    public TState DeserializeState<TState>(string json) where TState : FlowState, new()
    {
        try
        {
            return _inner.DeserializeState<TState>(json)
                   ?? throw new DeserializationException(
                       $"Deserialized state was null with type: '{typeof(TState)}' and json: '{MinifyJson(json)}'", 
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
                $"Unable to deserialize state with type: '{typeof(TState)}' and json: '{MinifyJson(json)}'",  
                e
            );
        }
    }
}