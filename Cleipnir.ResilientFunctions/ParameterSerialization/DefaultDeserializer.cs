using System;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public class DefaultDeserializer : IDeserializer
{
    public object DeserializeParameter(string json, string storedType, ParameterType parameterType, RFuncType rFuncType)
        => Json.Deserialize(json, Type.GetType(storedType, throwOnError: true)!);
}