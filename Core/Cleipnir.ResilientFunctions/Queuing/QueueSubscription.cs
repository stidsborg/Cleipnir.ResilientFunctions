using System;
using System.Collections.Generic;

namespace Cleipnir.ResilientFunctions.Queuing;

public class QueueSubscription<T>(IEnumerable<Func<object, bool>> predicates, QueueManager queueManager)
{
    
}