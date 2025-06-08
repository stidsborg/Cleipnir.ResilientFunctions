using System;
using System.Threading;

namespace Cleipnir.ResilientFunctions.Domain;

public class ReplicaId
{
    public Guid Id { get; }

    private int _offset;
    public int Offset
    {
        get
        {
            lock (_sync)
                return _offset;
        }
        set
        {
            lock (_sync)
                _offset = value;
        }
    } 

    private readonly Lock _sync = new();

    public ReplicaId(Guid id) => Id = id;
}