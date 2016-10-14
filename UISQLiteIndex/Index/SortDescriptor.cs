using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLiteIndex.Index
{
    class SortDescriptor
    {
        public SortDescriptor(string сolumnName, bool isAscending = true)
        {
            ColumnName = сolumnName;
            IsAscending = isAscending;
        }

        public String ColumnName { get; private set; }

        public bool IsAscending { get; private set; }
    }
}
