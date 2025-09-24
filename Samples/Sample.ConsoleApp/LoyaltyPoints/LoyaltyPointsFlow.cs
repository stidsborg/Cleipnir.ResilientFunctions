using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.LoyaltyPoints;

public class LoyaltyPointsFlow
{
    public static async Task Execute(string customerId, Workflow workflow)
    {
        var effect = workflow.Effect;
        var state = await effect.CreateOrGet("State", new LoyaltyPoints(DateTime.UtcNow, Points: 0));
        while (true)
        {
            var (date, points) = await Queue.Peek<LoyaltyPoints>();
            if (state.Date >= date)
            {
                await Queue.Pop();
                continue;
            }
            
            state = new LoyaltyPoints(Date: date, Points: state.Points + points);
            await effect.Upsert("State", state);
            
            await Queue.Pop();
        }
    }
    
    private record LoyaltyPoints(DateTime Date, int Points);
}