using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLiteIndex.Index
{
    static class SortDescriptorHelper
    {
        public static string GetSQLClauseRepresentation(IEnumerable<SortDescriptor> sortDescriptions)
        {
            var result = sortDescriptions.Aggregate<SortDescriptor, StringBuilder, String>(new StringBuilder(),
                (prevVal, sortDesc) => { prevVal.Append(String.Format("{0} {1},", sortDesc.ColumnName, sortDesc.IsAscending ? "ASC" : "DESC")); return prevVal; },
                (sb) => { return sb.Remove(sb.Length - 1, 1).ToString(); });
            return result;
        }
    }
}
