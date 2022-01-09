using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain
{
    public abstract class RScrapbook
    {
        private IFunctionStore? FunctionStore { get; set; }
        private FunctionId? FunctionId { get; set; }
        private int? Epoch { get; set; }

        public void Initialize(FunctionId functionId, IFunctionStore functionStore, int epoch)
        {
            FunctionId = functionId;
            FunctionStore = functionStore;
            Epoch = epoch;
        }
        
        public async Task Save()
        {
            //todo if not initialized throw framework exception
            
            var scrapbookJson= this.ToJson();
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
                throw new ScrapbookSaveFailedException("Unable to save Scrapbook due to unexpected version stamp");
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
}