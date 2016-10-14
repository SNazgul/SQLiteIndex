using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLiteIndex.Index.Utils
{
    static class IntegralUnboxing
    {
        public static Int64 ToInt64(object objVal)
        {
            return System.Convert.ToInt64(objVal);
        }
    }
}
