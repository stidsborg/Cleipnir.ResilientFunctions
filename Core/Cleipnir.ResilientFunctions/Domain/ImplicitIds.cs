namespace Cleipnir.ResilientFunctions.Domain;

public class ImplicitIds
{
    private int _next;
    public string Next() => (_next++).ToString();
}