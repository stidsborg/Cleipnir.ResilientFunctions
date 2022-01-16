namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public class DefaultSerializer : ISerializer
{
    public string Serialize(object parameter, ParameterType parameterType, RFuncType rFuncType)
        => parameter.ToJson();
}