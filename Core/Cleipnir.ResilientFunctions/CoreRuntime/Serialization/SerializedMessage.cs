using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public record SerializedMessage(
    StoredId StoredId,
    byte[] Content, 
    byte[] Type, 
    string? IdempotencyKey, 
    string? Sender, 
    string? Receiver
);

public record SerializedMessageWithReplicaId(
    SerializedMessage Message,
    ReplicaId ReplicaId
);