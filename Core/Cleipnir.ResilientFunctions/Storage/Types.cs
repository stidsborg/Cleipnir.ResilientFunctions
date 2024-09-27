using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoredFlow(
    FlowId FlowId,
    string? Parameter,
    string? DefaultState,
    Status Status,
    string? Result,
    StoredException? Exception,
    int Epoch,
    long Expires,
    long Timestamp,
    long InterruptCount,
    Guid Reference
);

public record IdAndEpoch(FlowId FlowId, int Epoch);

public record StoredException(string ExceptionMessage, string? ExceptionStackTrace, string ExceptionType);
public record StatusAndEpoch(Status Status, int Epoch);

public record StoredEffect(EffectId EffectId, WorkStatus WorkStatus, string? Result, StoredException? StoredException);
public record StoredState(StateId StateId, string StateJson);

public record IdWithParam(FlowId FlowId, string? Param);