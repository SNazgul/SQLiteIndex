using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteIndex.Index
{
    interface IDatabaseQueue : IDisposable
    {
        Task ExecuteAsync(Action<SQLiteConnection> act);

        void ExecuteSync(Action<SQLiteConnection> act);
    }
}
