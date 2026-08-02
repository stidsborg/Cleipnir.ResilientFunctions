using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal record RestartedFunction(
    StoredFlow StoredFlow,
    IReadOnlyList<StoredEffect> StoredEffects,
    IReadOnlyList<IncomingMessage> Messages,
    IStorageSession? StorageSession
);
