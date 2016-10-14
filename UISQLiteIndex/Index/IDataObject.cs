using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UISQLiteIndex.Index
{
    interface IDataObject
    {
        Object this[String key] { get;  set; }
    }
}
