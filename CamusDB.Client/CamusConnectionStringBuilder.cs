
namespace CamusDB.Client;

public class CamusConnectionStringBuilder
{
    public SessionPoolManager? SessionPoolManager { get; set; }

    public Dictionary<string, string> Config { get; } = new();

    public CamusConnectionStringBuilder(string connectionString)
    {
        string[] settings = connectionString.Split(";");

        foreach (string setting in settings)
        {
            string[] varParts = setting.Split("=");            

            Config.TryAdd(varParts[0], varParts[1]);

            //Console.WriteLine("{0} {1}", varParts[0], varParts[1]);
        }
    }
}


            