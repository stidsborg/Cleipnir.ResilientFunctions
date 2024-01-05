namespace Cleipnir.ResilientFunctions.Storage;

public static class ComplimentaryState
{
    public readonly record struct SaveScrapbookForExecutingFunction(StoredParameter StoredParameter, StoredScrapbook StoredScrapbook, long LeaseLength);
    public readonly record struct SetResult(StoredParameter StoredParameter, StoredScrapbook StoredScrapbook);
}