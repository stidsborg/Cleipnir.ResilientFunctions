using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

internal class SemaphoreFlow
{
    public async Task Run(int count, Workflow workflow)
    {
        var activeId = workflow.Effect.CreateNextImplicitId();
        var actives = await workflow.Effect.CreateOrGet(activeId, new List<Guid>(), alias: null, flush: false);
        var queuedId = workflow.Effect.CreateNextImplicitId();
        var queue = await workflow.Effect.CreateOrGet(queuedId, new List<Guid>(), alias: null, flush: false);

        while (true)
        {
            var x = await workflow.Message<string>();
            
        }

    }
    
}