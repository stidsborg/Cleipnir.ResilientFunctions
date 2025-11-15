using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Queuing;

public class QueueFlag
{
    private readonly Lock _sync = new();
    private TaskCompletionSource? _waiter = null;
    private bool _signaled;
    
    public Task WaitForRaised()
    {
        TaskCompletionSource waiter;
        lock (_sync)
            if (_signaled)
            {
                _signaled = false;
                return Task.CompletedTask;
            }
            else
            {
                waiter = _waiter = new();
            } 
        
        return waiter.Task;
    }

    public void Raise()
    {
        TaskCompletionSource? waiter = null;
        lock (_sync)
            if (_signaled)
                return;
            else if (_waiter == null) 
                _signaled = true;
            else
            {
                waiter = _waiter;
                _waiter = null;
            }
        
        waiter?.SetResult();
    }
}