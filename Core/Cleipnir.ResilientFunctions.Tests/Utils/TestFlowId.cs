using System;
using System.Runtime.CompilerServices;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class TestFlowId
{
    public static FlowId Create([CallerMemberName] string callerName = "") 
        => new FlowId(
            flowType: callerName + Random.Shared.Next(0, 5000),
            flowInstance: Guid.NewGuid().ToString("N")
        );
}

public static class TestStoredId
{
    public static StoredId Create() => StoredInstance
        .Create(Guid.NewGuid().ToString(), new StoredType(Random.Shared.Next(0, 10_000)))
        .ToStoredId();
    
    public static StoredId Create(StoredType type) => StoredInstance
        .Create(Guid.NewGuid().ToString(), type)
        .ToStoredId();
}