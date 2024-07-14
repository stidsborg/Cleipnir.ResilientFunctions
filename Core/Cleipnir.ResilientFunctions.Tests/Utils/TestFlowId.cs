using System;
using System.Runtime.CompilerServices;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class TestFlowId
{
    public static FlowId Create([CallerMemberName] string callerName = "") 
        => new FlowId(
            flowType: callerName + Random.Shared.Next(0, 5000),
            flowInstance: Guid.NewGuid().ToString("N")
        );
}