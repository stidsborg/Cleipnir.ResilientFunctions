using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public record SerializedMessage(
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