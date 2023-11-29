﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Extensions.Work;

namespace ConsoleApp.Engagement;

public static class EngagementReminderSaga
{
    public static async Task Start(StartCustomerEngagement startEngagement, RScrapbook scrapbook, Context context)
    {
        var (candidateEmail, nextReminderTime) = startEngagement;
        var es = context.EventSource;

        await es.DoAtLeastOnce(
            workId: "InitialCorrespondence",
            SendEngagementInitialCorrespondence
        );
        
        for (var i = 0; i < 10; i++)
        {
            //wait for candidate reply or timeout
            var either = await es
                .OfTypes<EngagementAccepted, EngagementRejected>()
                .Where(either =>
                    either.Match(
                        first: a => a.Iteration == i,
                        second: r => r.Iteration == i
                    )
                )
                .TakeUntilTimeout($"Timeout{i}", nextReminderTime)
                .SuspendUntilFirstOrDefault();
            
            if (either == null) //timeout
                continue;
            
            // if accepted notify hr and complete the flow
            if (either.Match(ea => true, er => false))
            {
                await scrapbook.DoAtLeastOnce(workId: "NotifyHR", work: () => NotifyHR(candidateEmail));
                await es.CancelTimeoutEvent(timeoutId: i.ToString());
                
                return;
            }

            //wait for timeout before sending next engagement reminder
            await es
                .TakeUntilTimeout($"Timeout{i}", nextReminderTime)
                .SuspendUntilFirst();
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