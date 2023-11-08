using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Extensions.Work;

namespace ConsoleApp.Engagement;

public static class EngagementSaga
{
    public static async Task Start(StartCustomerEngagement startEngagement, RScrapbook scrapbook, Context context)
    {
        var (candidateEmail, nextEngagementTime) = startEngagement;
        var es = await context.EventSource;

        await es.DoAtLeastOnce(
            workId: "InitialCorrespondence",
            SendEngagementInitialCorrespondence
        );
        
        for (var i = 0; i < 10; i++)
        {
            //register timeout
            await es.RegisterTimeoutEvent(timeoutId: i.ToString(), nextEngagementTime);
            
            //wait for candidate reply or timeout
            await es
                .OfTypes<EngagementAccepted, EngagementRejected, TimeoutEvent>()
                .Where(either =>
                    either.Match(
                        first: a => a.Iteration == i,
                        second: r => r.Iteration == i,
                        third: t => int.Parse(t.TimeoutId) == i
                    )
                )
                .SuspendUntilFirst();
            
            // if accepted notify hr and complete the flow
            if (es.Existing.OfType<EngagementAccepted>().Any())
            {
                await scrapbook.DoAtLeastOnce(workId: "NotifyHR", work: () => NotifyHR(candidateEmail));
                await es.CancelTimeoutEvent(timeoutId: i.ToString());
                
                return;
            }

            //wait for timeout before sending next engagement reminder
            await es
                .OfType<TimeoutEvent>()
                .Where(t => int.Parse(t.TimeoutId) == i)
                .SuspendUntilFirst();

            nextEngagementTime += TimeSpan.FromDays(1);
            
            await es.DoAtLeastOnce(
                workId: $"Reminder#{i}",
                SendEngagementReminder
            );
        }

        throw new Exception("Max number of retries exceeded");
    }

    private static Task NotifyHR(string candidateEmail) => Task.CompletedTask;
    private static Task SendEngagementInitialCorrespondence() => Task.CompletedTask;
    private static Task SendEngagementReminder() => Task.CompletedTask;
}

public record StartCustomerEngagement(string CandidateEmail, DateTime StartDate);
public record EngagementAccepted(int Iteration);
public record EngagementRejected(int Iteration);