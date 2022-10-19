using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain;
using static Cleipnir.ResilientFunctions.Helpers.Helpers;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public class ErrorHandlingDecorator : ISerializer
{
    private readonly ISerializer _inner;

    public ErrorHandlingDecorator(ISerializer inner) => _inner = inner;
    
    public string SerializeParameter(object parameter) => _inner.SerializeParameter(parameter);

    public TParam DeserializeParameter<TParam>(string json, string type)
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

    public string SerializeScrapbook(RScrapbook scrapbook) => _inner.SerializeScrapbook(scrapbook);

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

    public string SerializeError(RError error) => _inner.SerializeError(error);
    public RError DeserializeError(string json) => _inner.DeserializeError(json);

    public string SerializeResult(object result) => _inner.SerializeResult(result);
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
}