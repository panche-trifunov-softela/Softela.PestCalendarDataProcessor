using System.Data;
using Npgsql;

namespace Softela.PestCalendarDataProcessor.Data;

public sealed class DapperDataContext : IDapperDataContext, IAsyncDisposable
{
    private readonly NpgsqlConnection _connection;

    public DapperDataContext(string connectionString)
    {
        _connection = new NpgsqlConnection(connectionString);
    }

    public IDbConnection Connection => _connection;
    public IDbTransaction? Transaction { get; set; }

    public async ValueTask DisposeAsync()
    {
        if (Transaction is not null)
            await ((IAsyncDisposable)Transaction).DisposeAsync().ConfigureAwait(false);

        await _connection.DisposeAsync().ConfigureAwait(false);
    }
}
