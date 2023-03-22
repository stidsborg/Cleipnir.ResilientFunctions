using System;
using System.Runtime.CompilerServices;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class TestFunctionId
{
    public static FunctionId Create([CallerMemberName] string callerName = "") 
        => new FunctionId(
            functionTypeId: callerName + Random.Shared.Next(0, 5000),
            functionInstanceId: Guid.NewGuid().ToString("N")
        );
}