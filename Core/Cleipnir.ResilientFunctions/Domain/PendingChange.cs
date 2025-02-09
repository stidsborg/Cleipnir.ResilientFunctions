using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public record PendingChange(StoredEffectId Id, StoredEffect? StoredEffect);