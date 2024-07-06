using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Domain;

public abstract class FlowState
{
    private Func<Task>? OnSave { get; set; }
    
    private bool _initialized;

    public void Initialize(Func<Task> onSave)
    {
        ArgumentNullException.ThrowIfNull(onSave);
        
        if (_initialized)
            throw new InvalidOperationException("FlowState has already been initialized");
        
        _initialized = true;
        OnSave = onSave;
    }

    public virtual async Task Save()
    {
        if (!_initialized)
            throw new InvalidOperationException("FlowState must be initialized before save");
        
        await OnSave!.Invoke();  
    } 
}