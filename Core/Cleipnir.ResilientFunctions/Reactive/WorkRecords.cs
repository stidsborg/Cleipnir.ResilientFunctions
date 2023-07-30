namespace Cleipnir.ResilientFunctions.Reactive;

public record WorkStarted(string WorkId);
public record WorkCompleted(string WorkId);
public record WorkWithResultCompleted<T>(string WorkId, T Result) : WorkCompleted(WorkId);