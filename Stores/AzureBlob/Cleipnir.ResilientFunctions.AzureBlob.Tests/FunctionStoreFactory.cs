using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests;

public static class FunctionStoreFactory
{
    public static async Task<AzureBlobFunctionStore> CreateAndInitialize(string prefix)
    {
        var functionStore = new AzureBlobFunctionStore(Settings.ConnectionString!, prefix);
        await functionStore.Initialize();

        return functionStore;
    }   
}