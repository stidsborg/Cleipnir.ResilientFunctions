using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class PendingChange
{
    private StoredEffect? _storedEffect;
    public StoredEffect? StoredEffect
    {
        get => _storedEffect;
        set
        {
            _storedEffect = value;
            PendingOperation = _storedEffect == null
                ? PendingOperation.Delete
                : PendingOperation.Upsert;
        }
    }

    public required PendingOperation PendingOperation { get; set; }
}