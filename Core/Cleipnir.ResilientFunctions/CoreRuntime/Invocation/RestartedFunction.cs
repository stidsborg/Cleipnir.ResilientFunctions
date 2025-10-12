using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal record RestartedFunction(
    StoredFlow StoredFlow, 
    IReadOnlyList<StoredEffect> StoredEffects,
    IReadOnlyList<StoredMessage> StoredMessages,
    IStorageSession? StorageSession
);