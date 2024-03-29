﻿using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public interface ISerializer
{
    StoredParameter SerializeParameter<TParam>(TParam parameter) where TParam : notnull;
    TParam DeserializeParameter<TParam>(string json, string type) where TParam : notnull;
    StoredException SerializeException(Exception exception);
    PreviouslyThrownException DeserializeException(StoredException storedException);
    StoredResult SerializeResult<TResult>(TResult result);
    TResult DeserializeResult<TResult>(string json, string type);
    JsonAndType SerializeMessage<TEvent>(TEvent @event) where TEvent : notnull;
    object DeserializeMessage(string json, string type);
    string SerializeEffectResult<TResult>(TResult result);
    TResult DeserializeEffectResult<TResult>(string json);
}