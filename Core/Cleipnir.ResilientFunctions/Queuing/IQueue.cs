using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;

namespace Cleipnir.ResilientFunctions.Queuing;

public interface IQueue
{
    Task<T> Next<T>() where T : notnull;
}

public interface IQueueBuilder<T>
{
    public IQueueBuilder<T> Where(Func<T, bool> predicate);
    public IQueueBuilder<T2> OfType<T2>() where T2 : T;
}

public class QueueBuilder<T>(IEnumerable<Func<object, bool>> predicates)
{
    public QueueBuilder<T> Where(Func<T, bool> predicate)
        => new(
            predicates.Append(msg => msg is T && predicate((T)msg))
        );
    
    public QueueBuilder<TChild> OfType<TChild>() where TChild : T 
        => new(predicates.Append(msg => msg is TChild));
}