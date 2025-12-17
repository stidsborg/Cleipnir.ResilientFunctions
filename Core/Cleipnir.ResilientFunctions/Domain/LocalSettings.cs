using System;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;

namespace Cleipnir.ResilientFunctions.Domain;

public class LocalSettings
{
    internal TimeSpan? RetentionPeriod { get; }
    internal bool? EnableWatchdogs { get; }
    internal int? MaxParallelRetryInvocations { get; }
    public TimeSpan? MessagesDefaultMaxWaitForCompletion { get; }
    public bool ClearChildrenAfterCapture { get; }

    public LocalSettings(
        TimeSpan? retentionPeriod = null,
        bool? enableWatchdogs = null,
        TimeSpan? messagesDefaultMaxWaitForCompletion = null,  
        int? maxParallelRetryInvocations = null,
        bool clearChildrenAfterCapture = false)
    {
        RetentionPeriod = retentionPeriod;
        EnableWatchdogs = enableWatchdogs;
        MaxParallelRetryInvocations = maxParallelRetryInvocations;
        MessagesDefaultMaxWaitForCompletion = messagesDefaultMaxWaitForCompletion;
        ClearChildrenAfterCapture = clearChildrenAfterCapture;
    }
}