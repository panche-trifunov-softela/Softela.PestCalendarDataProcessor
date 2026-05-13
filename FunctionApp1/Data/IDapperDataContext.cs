using System.Data;

namespace Softela.PestCalendarDataProcessor.Data;

public interface IDapperDataContext
{
    IDbConnection? Connection { get; }
    IDbTransaction? Transaction { get; set; }
}
