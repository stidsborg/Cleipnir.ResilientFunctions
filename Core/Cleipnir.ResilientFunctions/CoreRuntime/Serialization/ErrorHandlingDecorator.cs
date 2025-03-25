﻿using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public class ErrorHandlingDecorator(ISerializer inner) : ISerializer
{
    public byte[] Serialize<T>(T value) => inner.Serialize(value);
    public byte[] Serialize(object? value, Type type) => inner.Serialize(value, type);

    public T Deserialize<T>(byte[] bytes)
    {
        try
        {
            return inner.Deserialize<T>(bytes);
        }
        catch (DeserializationException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new DeserializationException(
                $"Unable to deserialize value with type: '{typeof(T).SimpleQualifiedName()}' and bytes: '{Convert.ToBase64String(bytes)}'", 
                e
            );
        }
    }
    
    public StoredException SerializeException(FatalWorkflowException exception)
        => inner.SerializeException(exception);

    public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException)
    {
        try
        {
            return inner.DeserializeException(flowId, storedException);
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

    public SerializedMessage SerializeMessage(object message, Type messageType)
        => inner.SerializeMessage(message, messageType);
    public object DeserializeMessage(byte[] json, byte[] type)
    {
        try
        {
            return inner.DeserializeMessage(json, type)
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
}

public static class ErrorHandlingDecoratorExtensions
{
    public static ISerializer DecorateWithErrorHandling(this ISerializer serializer)
        => new ErrorHandlingDecorator(serializer);
}