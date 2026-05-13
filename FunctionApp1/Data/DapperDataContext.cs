using System.Data;
using Npgsql;

namespace Softela.PestCalendarDataProcessor.Data;

public sealed class DapperDataContext : IDapperDataContext, IDisposable
{
    private readonly NpgsqlConnection _connection;

    public DapperDataContext(string connectionString)
    {
        _connection = new NpgsqlConnection(connectionString);
        _connection.Open();
    }

    public IDbConnection? Connection => _connection;
    public IDbTransaction? Transaction { get; set; }

    public void Dispose()
    {
        Transaction?.Dispose();
        _connection.Dispose();
    }
}
