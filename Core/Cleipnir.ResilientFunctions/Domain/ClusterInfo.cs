using System.Threading;

namespace Cleipnir.ResilientFunctions.Domain;

public class ClusterInfo
{
    public ReplicaId ReplicaId { get; }

    private int _offset;
    public int Offset
    {
        get
        {
            lock (_sync)
                return _offset;
        }
        internal set
        {
            lock (_sync)
                _offset = value;
        }
    }

    private int _replicaCount;
    public int ReplicaCount
    {
        get
        {
            lock (_sync)
                return _replicaCount;
        }
        internal set
        {
            lock (_sync)
                _replicaCount = value;
        }
    }

    private readonly Lock _sync = new();

    public ClusterInfo(ReplicaId replicaId) => ReplicaId = replicaId;
}