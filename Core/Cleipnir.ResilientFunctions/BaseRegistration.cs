using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public abstract class BaseRegistration
{
    public StoredType StoredType { get; }
    protected UtcNow UtcNow { get; }

    protected BaseRegistration(StoredType storedType, UtcNow utcNow)
    {
        StoredType = storedType;
        UtcNow = utcNow;
    }

    public StoredId MapToStoredId(FlowInstance instance) => StoredId.Create(StoredType, instance.Value);
}
