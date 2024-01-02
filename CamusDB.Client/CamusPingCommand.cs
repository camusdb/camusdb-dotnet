
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
