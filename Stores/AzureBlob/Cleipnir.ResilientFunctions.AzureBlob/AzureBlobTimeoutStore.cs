using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public class AzureBlobTimeoutStore : ITimeoutStore
{
    private readonly string _connectionString;

    public AzureBlobTimeoutStore(string connectionString)
    {
        _connectionString = connectionString;
    }

    public Task Initialize()
    {
        throw new NotImplementedException();
    }

    public Task UpsertTimeout(StoredTimeout storedTimeout)
    {
        throw new NotImplementedException();
    }

    public Task RemoveTimeout(FunctionId functionId, string timeoutId)
    {
        throw new NotImplementedException();
    }

    public Task<IEnumerable<StoredTimeout>> GetTimeouts(string functionTypeId, long expiresBefore)
    {
        throw new NotImplementedException();
    }
}