using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Newtonsoft.Json;

namespace Cleipnir.ResilientFunctions.Domain
{
    public abstract class RScrapbook
    {
        private IFunctionStore? FunctionStore { get; set; }
        private FunctionId? FunctionId { get; set; }
        private int VersionStamp { get; set; }

        public void Initialize(FunctionId functionId, IFunctionStore functionStore, int versionStamp)
        {
            FunctionId = functionId;
            FunctionStore = functionStore;
            VersionStamp = versionStamp;
        }
        
        public async Task Save()
        {
            var stateJson = JsonConvert.SerializeObject(this);
            var success = await FunctionStore!.UpdateScrapbook(
                FunctionId!, 
                stateJson, 
                VersionStamp, VersionStamp + 1
            );
            
            if (!success)
                throw new ScrapbookSaveFailedException("Unable to save Scrapbook due to unexpected version stamp");

            VersionStamp++;
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