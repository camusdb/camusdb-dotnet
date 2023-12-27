
namespace CamusDB.Client;

public class SessionPoolManager
{
    public static SessionPoolManager Create(SessionPoolOptions options)
    {
        return new SessionPoolManager();
    }
}
