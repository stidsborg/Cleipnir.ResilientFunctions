﻿using Serilog;

namespace Sample.Kodedyret.V1;

public interface IEmailClient
{
    Task SendOrderConfirmation(Guid customerId, IEnumerable<Guid> productIds);
}

public class EmailClientStub : IEmailClient
{
    public Task SendOrderConfirmation(Guid customerId, IEnumerable<Guid> productIds)
        => Task.Delay(100).ContinueWith(_ => 
            Log.Logger.ForContext<IEmailClient>().Information("EMAIL_SERVER: Order confirmation emailed")
        );
}