﻿using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public interface ISerializer
{
    string SerializeParameter(object parameter);
    TParam DeserializeParameter<TParam>(string json, string type);
    string SerializeScrapbook(RScrapbook scrapbook);
    TScrapbook DeserializeScrapbook<TScrapbook>(string? json, string type) where TScrapbook : RScrapbook;
    string SerializeScrapbooks(IEnumerable<OwnedScrapbook> scrapbooks);
    Dictionary<string, RScrapbook> DeserializeScrapbooks(string json);
    string SerializeError(RError error);
    RError DeserializeError(string json);
    string SerializeResult(object result);
    TResult DeserializeResult<TResult>(string json, string type);
}