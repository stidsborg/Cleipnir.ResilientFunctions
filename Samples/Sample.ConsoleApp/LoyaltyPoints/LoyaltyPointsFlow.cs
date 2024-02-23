using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.LoyaltyPoints;

public class LoyaltyPointsFlow
{
    public static async Task Execute(string customerId, State state)
    {
        while (true)
        {
            var (date, points) = await Queue.Peek<LoyaltyPoints>();
            if (state.LatestDate >= date)
            {
                await Queue.Pop();
                continue;
            }
            
            state.LoyaltyPoints += points;
            state.LatestDate = date;
            await state.Save();
            
            await Queue.Pop();
        }
    }
    
    private record LoyaltyPoints(DateTime Date, int Points);

    public class State : WorkflowState
    {
        public int LoyaltyPoints { get; set; }
        public DateTime LatestDate { get; set; }
    }
}