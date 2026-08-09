using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public record PendingEffectChange(EffectId Id, DeserializedEffect? Effect, StoredEffect? StoredEffect, CrudOperation? Operation, bool Existing, string? Alias);