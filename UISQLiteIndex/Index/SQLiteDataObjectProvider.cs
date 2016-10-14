using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using UISQLiteIndex.Index;

namespace SQLiteIndex.Index
{
    class SQLiteDataObjectProvider : ISlimDataObjectProvider
    {
        private String _tableName;
        private IDatabaseQueue _dbQueue;
        private List<string> _columnTitles;

        public SQLiteDataObjectProvider(String tableName, IDatabaseQueue dbQueue)
        {
            _tableName = tableName;
            _dbQueue = dbQueue;
        }

        public String TableName
        {
            get { return _tableName; }
        }

        public IDataObject CreateDataObjectFromSQLReader(SQLiteDataReader reader, bool readIdOnly)
        {
            IDataObject result = null;
            if (readIdOnly)
            {
                int idx = ColumnTitles.IndexOf("ID");
                if (idx == -1)
                    throw new DataIsNotAvailableException("ID field doesn't found", null);
                result = new DataObject();
                result["ID"] = idx;
            }
            else
            {
                for(int i = 0; i < reader.FieldCount; ++i)
            }
            return result;
        }

        public void EnsureExistingIndex(SQLiteConnection conn, IEnumerable<SortDescriptor> sortDescriptions)
        {
            throw new NotImplementedException();
        }

        public void StopUsingIndex(SQLiteConnection conn, IEnumerable<SortDescriptor> _sortDescriptions)
        {
            throw new NotImplementedException();
        }

        protected IList<string> ColumnTitles
        {
            get
            {
                if (_columnTitles == null)
                {

                }

                return _columnTitles;
            }
        }

        public IDatabaseQueue LongOperationQueue
        {
            get
            {
                return _dbQueue;
            }            
        }
    }
}
