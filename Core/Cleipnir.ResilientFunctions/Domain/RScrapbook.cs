using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

namespace Cleipnir.ResilientFunctions.Domain;

public class RScrapbook
{
    private Func<Task>? OnSave { get; set; }
    private readonly AsyncSemaphore _semaphore = new(maxParallelism: 1);
    public Dictionary<string, string> StateDictionary { get; set; } = new(); //allows for state accessible from middleware etc

    public void Initialize(Func<Task> onSave) => OnSave = onSave;

    public virtual async Task Save() => await (OnSave?.Invoke() ?? Task.CompletedTask);
    public Task<IDisposable> Lock() => _semaphore.Take();
}

public sealed class ScrapbookSaveFailedException : Exception
{
    public FunctionId FunctionId { get; }
    
    public ScrapbookSaveFailedException(FunctionId functionId, string message) : base(message) 
        => FunctionId = functionId;
}