using System;
using System.Threading;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public static class CurrentFlow
{
    public static StoredId StoredId
    {
        get
        {
            var currentFlowId = _workflow.Value?.StoredId;;
            if (currentFlowId is null)
                throw new InvalidOperationException("Unable to determine current flow. Flow must be invoked through the framework");

            return currentFlowId;
        }
    }
    
    internal static readonly AsyncLocal<Workflow?> _workflow = new();
    public static Workflow? Workflow => _workflow.Value;
}