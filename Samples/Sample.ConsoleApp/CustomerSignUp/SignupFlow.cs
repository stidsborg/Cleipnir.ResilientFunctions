﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.CustomerSignUp;

public static class SignupFlow
{
    public static async Task Start(string customerEmail, Workflow workflow)
    {
        var (effect, messages) = workflow;
        
        await effect.Capture(() => SendWelcomeMail(customerEmail));

        for (var i = 0; i <= 5; i++)
        {
            var emailVerifiedOption = await messages
                .TakeUntilTimeout(TimeSpan.FromDays(1))
                .OfType<EmailVerified>()
                .FirstOrNone();
            
            if (emailVerifiedOption.HasValue)
                break;
            
            if (i == 5)
                throw new UserSignupFailedException($"User '{customerEmail}' did not activate within 5 reminder emails");
            
            await effect.Capture(() => SendReminderMail(customerEmail));
        }
        
        await effect.Capture(() => SendFollowUpMail(customerEmail));
    }

    private static Task SendWelcomeMail(string customerEmail) => Task.CompletedTask;
    private static Task SendReminderMail(string customerEmail) => Task.CompletedTask;
    private static Task SendFollowUpMail(string customerEmail) => Task.CompletedTask;

    public class UserSignupFailedException : Exception
    {
        public UserSignupFailedException(string? message) : base(message) {}
    }
}

public record EmailVerified(string EmailAddress);