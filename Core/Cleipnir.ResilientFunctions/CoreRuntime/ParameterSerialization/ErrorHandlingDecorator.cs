using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using static Cleipnir.ResilientFunctions.Helpers.Helpers;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public class ErrorHandlingDecorator : ISerializer
{
    private readonly ISerializer _inner;

    public ErrorHandlingDecorator(ISerializer inner) => _inner = inner;
    
    public StoredParameter SerializeParameter<TParam>(TParam parameter) where TParam : notnull 
        => _inner.SerializeParameter(parameter);
    public TParam DeserializeParameter<TParam>(string json, string type) where TParam : notnull
    {
        try
        {
            return _inner.DeserializeParameter<TParam>(json, type)
                   ?? throw new DeserializationException(
                       $"Deserialized parameter was null with type: '{type}' and json: '{MinifyJson(json)}'", 
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
                $"Unable to deserialize parameter with type: '{type}' and json: '{MinifyJson(json)}'", 
                e
            );
        }
    }

    public StoredScrapbook SerializeScrapbook<TScrapbook>(TScrapbook scrapbook) where TScrapbook : RScrapbook
        => _inner.SerializeScrapbook(scrapbook);
    public TScrapbook DeserializeScrapbook<TScrapbook>(string json, string type) where TScrapbook : RScrapbook
    {
        try
        {
            return _inner.DeserializeScrapbook<TScrapbook>(json, type);
        }
        catch (DeserializationException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new DeserializationException(
                $"Unable to deserialize scrapbook with type: '{type}' and json: '{MinifyJson(json)}'", 
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

    public StoredResult SerializeResult<TResult>(TResult result)
        => _inner.SerializeResult(result);
    public TResult DeserializeResult<TResult>(string json, string type)
    {
        try
        {
            return _inner.DeserializeResult<TResult>(json, type)
                   ?? throw new DeserializationException(
                       $"Deserialized result was null with type: '{type}' and json: '{MinifyJson(json)}'", 
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
                $"Unable to deserialize result with type: '{type}' and json: '{MinifyJson(json)}'", 
                e
            );
        }
    }

    public JsonAndType SerializeEvent<TEvent>(TEvent @event) where TEvent : notnull
        => _inner.SerializeEvent(@event);
    public object DeserializeEvent(string json, string type)
    {
        try
        {
            return _inner.DeserializeEvent(json, type)
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

    public string SerializeActivityResult<TResult>(TResult result)
        => _inner.SerializeActivityResult(result);
    public TResult DeserializeActivityResult<TResult>(string json)
    {
        try
        {
            return _inner.DeserializeActivityResult<TResult>(json)
                   ?? throw new DeserializationException(
                       $"Deserialized activity result was null with type: '{typeof(TResult)}' and json: '{MinifyJson(json)}'", 
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
                $"Deserialized activity result was null with type: '{typeof(TResult)}' and json: '{MinifyJson(json)}'",  
                e
            );
        }
    }
}