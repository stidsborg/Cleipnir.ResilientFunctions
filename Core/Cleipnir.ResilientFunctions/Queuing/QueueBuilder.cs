using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Queuing;

public class QueueBuilder<T>(IEnumerable<Func<object, bool>> predicates, QueueManager manager)
{
    internal bool CanHandle(object msg)
    {
        if (msg is not T)
            return false;

        return predicates.All(f => f(msg));
    }
    
    public QueueBuilder<T> Where(Func<T, bool> predicate)
        => new(
            predicates.Append(msg => msg is T && predicate((T)msg)),
            manager
        );

    public QueueBuilder<TChild> OfType<TChild>() where TChild : T
        => new QueueBuilder<TChild>(predicates, manager);
}