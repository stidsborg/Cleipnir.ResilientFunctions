using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

// A null Type marks an empty restart-poke - it carries no payload (see MessageSender.SendRestartPokes).
public record SerializedMessage(
    StoredId StoredId,
    byte[] Content,
    TypeId? Type,
    string? IdempotencyKey,
    string? Sender,
    string? Receiver
);

public record SerializedMessageWithReplicaId(
    SerializedMessage Message,
    ReplicaId ReplicaId
);