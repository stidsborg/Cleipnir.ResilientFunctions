using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Domain;

public class RScrapbook
{
    private Func<Task>? OnSave { get; set; }
    public Dictionary<string, string> StateDictionary { get; set; } = new(); //allows for state accessible from middleware etc
    
    public void Initialize(Func<Task> onSave) => OnSave = onSave;

    public virtual async Task Save() 
    {
        await (OnSave?.Invoke() ?? Task.CompletedTask);
    }
}

public sealed class ScrapbookSaveFailedException : Exception
{
    public FunctionId FunctionId { get; }
    
    public ScrapbookSaveFailedException(FunctionId functionId, string message) : base(message) 
        => FunctionId = functionId;
}