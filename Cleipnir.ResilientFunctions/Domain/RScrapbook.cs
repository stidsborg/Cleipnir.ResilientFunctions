using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public abstract class RScrapbook
{
    private IFunctionStore? FunctionStore { get; set; }
    private ISerializer? Serializer { get; set; }
    private FunctionId? FunctionId { get; set; }
    private int? Epoch { get; set; }

    public void Initialize(FunctionId functionId, IFunctionStore functionStore, ISerializer serializer, int epoch)
    {
        FunctionId = functionId;
        FunctionStore = functionStore;
        Serializer = serializer;
        Epoch = epoch;
    }
        
    public async Task Save()
    {
        if (FunctionStore == null)
            throw new FrameworkException($"'{GetType().Name}' scrapbook was uninitialized on save");

        var scrapbookJson = Serializer!.SerializeScrapbook(this);
        var success = await FunctionStore!.SetFunctionState(
            FunctionId!,
            Status.Executing,
            scrapbookJson,
            failed: null,
            result: null,
            postponedUntil: null,
            expectedEpoch: Epoch!.Value
        );
            
        if (!success)
            throw new ScrapbookSaveFailedException("Unable to save Scrapbook due to concurrent modification");
    }
}

public sealed class ScrapbookSaveFailedException : Exception
{
    public ScrapbookSaveFailedException() { }

    public ScrapbookSaveFailedException(SerializationInfo info, StreamingContext context) 
        : base(info, context) { }

    public ScrapbookSaveFailedException(string? message) : base(message) { }

    public ScrapbookSaveFailedException(string? message, Exception? innerException) 
        : base(message, innerException) { }
}