namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public interface ISerializer
{
    string Serialize(object parameter, ParameterType parameterType, RFuncType rFuncType);
}