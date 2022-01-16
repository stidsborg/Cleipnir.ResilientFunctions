namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public interface IDeserializer
{
    public object DeserializeParameter(
        string json,
        string storedParameterType,
        ParameterType parameterType,
        RFuncType rFuncType
    );
}