
using Flurl.Http;

namespace CamusDB.Client;

public class CamusPingCommand : CamusCommand
{
    public CamusPingCommand(string source, CamusConnectionStringBuilder builder) : base(source, builder)
    {

    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        string endpoint = builder.Config["Endpoint"];

        var response = await endpoint
                                .WithTimeout(10)
                                .AppendPathSegments("ping")
                                .GetJsonAsync();

        Console.WriteLine(response);

        return 1;
    }
}
