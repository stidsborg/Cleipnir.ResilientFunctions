using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class RScrapbook
{
    private IFunctionStore? FunctionStore { get; set; }
    private ISerializer? Serializer { get; set; }
    private FunctionId? FunctionId { get; set; }
    private int? Epoch { get; set; }

    public Dictionary<string, string> StateDictionary { get; set; } = new(); //allows for state accessible from middleware etc

    public void Initialize(FunctionId functionId, IFunctionStore functionStore, ISerializer serializer, int epoch)
    {
        FunctionId = functionId;
        FunctionStore = functionStore;
        Serializer = serializer;
        Epoch = epoch;
    }
        
    public virtual async Task Save()
    {
        if (FunctionStore == null)
            throw new InvalidOperationException($"'{GetType().Name}' scrapbook was uninitialized on save");

        var scrapbookJson = Serializer!.SerializeScrapbook(this);
        var success = await FunctionStore!.SetScrapbook(FunctionId!, scrapbookJson, Epoch!.Value);

        if (!success)
            throw new ScrapbookSaveFailedException(
                FunctionId!,
                $"Unable to save '{FunctionId}'-scrapbook due to concurrent modification"
            );
    }
}

public sealed class ScrapbookSaveFailedException : Exception
{
    public FunctionId FunctionId { get; }
    
    public ScrapbookSaveFailedException(FunctionId functionId, string message) : base(message) 
        => FunctionId = functionId;
}