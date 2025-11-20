using System.Threading;

namespace Cleipnir.ResilientFunctions.Domain;

public class EffectContext
{
    public EffectId? Parent { get; }
    private int ImplicitId { get; set; }

    private static readonly AsyncLocal<EffectContext> Context = new();
    
    private EffectContext(EffectId parent)
    {
        Parent = parent;
        ImplicitId = -1;
    }

    private EffectContext()
    {
        Parent = null;
        ImplicitId = -1;
    }
    
    public static EffectContext Empty => new();

    public static EffectContext CurrentContext
    {
        get
        {
            var context = Context.Value;
            if (context == null)
            {
                context = new EffectContext();
                Context.Value = context;
            }
            
            return context;    
        }
    }
    
    public static void SetParent(EffectId parentId) => Context.Value = new EffectContext(parentId);

    public int NextImplicitId() => ++ImplicitId;
}