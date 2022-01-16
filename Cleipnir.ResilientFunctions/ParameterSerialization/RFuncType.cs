using System;
using System.Reflection;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public record RFuncType(
    Type ParameterType,
    Type? ScrapbookType,
    Type? ReturnValueType,
    MethodInfo RFuncMethodInfo
);