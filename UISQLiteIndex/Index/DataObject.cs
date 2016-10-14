using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UISQLiteIndex.Index
{
    class DataObject : IDataObject
    {
        Dictionary<string, object> _values;
        public Object this[String key]
        {
            get
            {
                Object res = null;
                _values.TryGetValue(key, out res);
                return res;
            }

            set
            {
                _values[key] = value;
            }
        }
    }
}
