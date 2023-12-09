using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Domain;

public class RScrapbook
{
    private Func<Task>? OnSave { get; set; }
    public ConcurrentDictionary<string, string> StateDictionary { get; set; } = new(); //allows for state accessible from middleware etc
    
    private bool _initialized;

    public void Initialize(Func<Task> onSave)
    {
        ArgumentNullException.ThrowIfNull(onSave);
        
        if (_initialized)
            throw new InvalidOperationException("Scrapbook has already been initialized");
        
        _initialized = true;
        OnSave = onSave;
    }

    public virtual async Task Save()
    {
        if (!_initialized)
            throw new InvalidOperationException("Scrapbook must be initialized before save");
        
        await OnSave!.Invoke();  
    } 
}

public sealed class ScrapbookSaveFailedException : Exception
{
    public FunctionId FunctionId { get; }
    
    public ScrapbookSaveFailedException(FunctionId functionId, string message) : base(message) 
        => FunctionId = functionId;
}