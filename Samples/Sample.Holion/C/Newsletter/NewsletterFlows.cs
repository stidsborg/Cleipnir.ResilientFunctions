using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Sample.Holion.C.Newsletter;

public class NewsletterFlows
{
    private readonly RAction<MailAndRecipients, NewsletterFlow.NewsletterScrapbook> _registration;
    public NewsletterFlows(RFunctions rFunctions)
    {
        _registration = rFunctions
            .RegisterMethod<NewsletterFlow>()
            .RegisterAction<MailAndRecipients, NewsletterFlow.NewsletterScrapbook>(
                functionTypeId: nameof(NewsletterFlow),
                flow => (param, scrapbook, context) => PrepareAndExecute(flow, param, scrapbook, context)
            );
    }
    
    public Task<ControlPanel<MailAndRecipients, NewsletterFlow.NewsletterScrapbook>?> ControlPanel(string instanceId) => _registration.ControlPanels.For(instanceId);
    public EventSourceWriter EventSourceWriter(string instanceId) => _registration.EventSourceWriters.For(instanceId);

    public Task Run(string instanceId, MailAndRecipients mailAndRecipients, NewsletterFlow.NewsletterScrapbook? scrapbook = null)
        => _registration.Invoke(instanceId, mailAndRecipients, scrapbook);

    public Task Schedule(string instanceId, MailAndRecipients mailAndRecipients, NewsletterFlow.NewsletterScrapbook? scrapbook = null) 
        => _registration.Schedule(instanceId, mailAndRecipients, scrapbook);

    private async Task PrepareAndExecute(NewsletterFlow flow, MailAndRecipients order, NewsletterFlow.NewsletterScrapbook scrapbook, Context context)
    {
        typeof(NewsletterFlow).GetProperty(nameof(Flow<Unit>.Context))!.SetValue(flow, context);
        typeof(NewsletterFlow).GetProperty(nameof(Flow<Unit>.Scrapbook))!.SetValue(flow, scrapbook);
        var eventSource = await context.EventSource;
        typeof(NewsletterFlow).GetProperty(nameof(Flow<Unit>.EventSource))!.SetValue(flow, eventSource);
        typeof(NewsletterFlow).GetProperty(nameof(Flow<Unit>.Utilities))!.SetValue(flow, context.Utilities);

        await flow.Run(order);
    }
}