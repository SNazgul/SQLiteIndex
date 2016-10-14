using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;

namespace SQLiteIndex.Index
{
    interface ISlimDataObjectProvider
    {
        IDatabaseQueue LongOperationQueue { get; }

        void EnsureExistingIndex(SQLiteConnection conn, IEnumerable<SortDescriptor> sortDescriptions);

        void StopUsingIndex(SQLiteConnection conn, IEnumerable<SortDescriptor> _sortDescriptions);
    }
}
