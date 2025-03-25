using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public record PendingEffectChange(StoredEffectId Id, StoredEffect? StoredEffect, CrudOperation? Operation, bool Existing);