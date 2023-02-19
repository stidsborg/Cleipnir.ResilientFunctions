namespace Cleipnir.ResilientFunctions.Storage;

public static class ComplimentaryState
{
    public readonly record struct SaveScrapbookForExecutingFunction(StoredParameter StoredParameter, StoredScrapbook StoredScrapbook);
}