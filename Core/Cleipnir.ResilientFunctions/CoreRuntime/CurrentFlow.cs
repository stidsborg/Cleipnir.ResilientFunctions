using System;
using System.Threading;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public static class CurrentFlow
{
    public static StoredId StoredId
    {
        get
        {
            var currentFlow = _id.Value;
            if (currentFlow is null)
                throw new InvalidOperationException("Unable to determine current flow. Flow must be invoked through the framework");

            return currentFlow;
        }
    }

    internal static readonly AsyncLocal<StoredId?> _id = new();
}