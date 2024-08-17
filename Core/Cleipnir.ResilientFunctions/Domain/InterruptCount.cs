using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Domain;

public struct InterruptCount
{
    private readonly Func<Task<long>> _getLatestInterrupt;
    public long Value { get; }
    
    public InterruptCount(long value, Func<Task<long>> getLatestInterrupt)
    {
        _getLatestInterrupt = getLatestInterrupt;
        Value = value;
    }

    public async Task<InterruptCount> GetLatest()
    {
        var latestInterrupt = await _getLatestInterrupt();
        return new InterruptCount(latestInterrupt, _getLatestInterrupt);
    }
}