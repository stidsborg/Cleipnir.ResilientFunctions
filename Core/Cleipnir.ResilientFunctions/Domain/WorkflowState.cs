using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Domain;

public abstract class WorkflowState
{
    private Func<Task>? OnSave { get; set; }
    
    private bool _initialized;

    public void Initialize(Func<Task> onSave)
    {
        ArgumentNullException.ThrowIfNull(onSave);
        
        if (_initialized)
            throw new InvalidOperationException("State has already been initialized");
        
        _initialized = true;
        OnSave = onSave;
    }

    public virtual async Task Save()
    {
        if (!_initialized)
            throw new InvalidOperationException("State must be initialized before save");
        
        await OnSave!.Invoke();  
    } 
}