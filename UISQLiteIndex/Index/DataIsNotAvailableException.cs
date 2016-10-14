using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UISQLiteIndex.Index
{
    class DataIsNotAvailableException : Exception
    {
        public DataIsNotAvailableException(String msg, Exception inner)
            : base(msg, inner)
        {

        }
    }
}
