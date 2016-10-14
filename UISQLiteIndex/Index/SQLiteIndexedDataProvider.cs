using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SQLite;
using System.Threading.Tasks;
using UISQLiteIndex.Index;

namespace SQLiteIndex.Index
{
    class SQLiteIndexedDataProvider : ISortedFilteredDataProvider
    {
        SQLiteDataObjectProvider _dataProvider;
        IEnumerable<SortDescriptor> _sortDescriptions;
        string _tempTableWithFilteredIDs;
        long? _count;


        /// <param name="sortDescriptions">Columns which will be sorted, considering the order of SortDescriptor objects in sequences</param>
        /// <exception cref="ArgumentNullException">Will be thrown if dataProvider or sortDescriptions are eqauls null</exception>
        /// <exception cref="ArgumentException ">Will be thrown if sortDescriptions is empty</exception>
        public SQLiteIndexedDataProvider(SQLiteDataObjectProvider dataProvider, IEnumerable<SortDescriptor> sortDescriptions)
        {
            if (dataProvider == null)
                throw new ArgumentNullException("'dataProvider' parameter is null");

            if (sortDescriptions == null)
                throw new ArgumentNullException("'sortDescriptions' parameter is null");

            if (!sortDescriptions.GetEnumerator().MoveNext())
                throw new ArgumentException("'sortDescriptions' is empty");

            _dataProvider = dataProvider;
            _sortDescriptions = sortDescriptions;
        }

        public bool IsSorted { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns>Task, that will finish when ISortedDataProvider will have been created successfully or if an error will have been occurred</returns>
        /// <remarks>Task can throw DataIsNotAvailableException, see inner exception to get more detailed information</remarks>
        public Task Sort()
        {
            Task t = Queue.ExecuteAsync(
                (conn) =>
                {
                    ((ISlimDataObjectProvider)_dataProvider).EnsureExistingIndex(conn, _sortDescriptions);

                    IsSorted = true;
                });

            return t;
        }


        #region ISortedDataProvider

        public IEnumerable<SortDescriptor> SortOptions
        {
            get
            {
                return _sortDescriptions;
            }
        }

        public long Count
        {
            get
            {
                long result = 0;

                try
                {
                    Queue.ExecuteSync(
                    (conn) =>
                    {
                        using (SQLiteCommand cmd = new SQLiteCommand(conn))
                        {
                            cmd.CommandText = String.Format("SELECT count() FROM {0} {1}", _dataProvider.TableName, GetJoinClause(_dataProvider.TableName));
                            object val = cmd.ExecuteScalar();

                            result = Utils.IntegralUnboxing.ToInt64(val);
                        }
                    });
                }
                catch (SQLiteException)
                {
                }
                catch (InvalidCastException)
                {
                }

                return result;
            }
        }


        private long GetObjectIdByIndex(SQLiteConnection conn, long index)
        {
            long result = -1;

            String cmdText = String.Format("SELECT {0}.ID FROM {0} {1} ORDER BY {2} LIMIT ?,1",
                _dataProvider.TableName,
                GetJoinClause(_dataProvider.TableName),
                AppendOrderByID(SortDescriptorHelper.GetSQLClauseRepresentation(_sortDescriptions), _dataProvider.TableName));
            using (SQLiteCommand cmd = new SQLiteCommand(conn))
            {
                cmd.CommandText = cmdText;

                SQLiteParameter sqLiteParam = new SQLiteParameter();
                sqLiteParam.Value = index;
                cmd.Parameters.Add(sqLiteParam);

                object val = cmd.ExecuteScalar();
                if (val != null && !DBNull.Value.Equals(val))
                    result = Utils.IntegralUnboxing.ToInt64(val);
            }

            return result;
        }

        public long GetObjectIdByIndex(long index)
        {
            if (index < 0)
                return -1;

            long result = -1;
            try
            {
                Queue.ExecuteSync(
                (conn) =>
                {
                    result = GetObjectIdByIndex(conn, index);
                });
            }
            catch (SQLiteException)
            {
            }
            catch (InvalidCastException)
            {
            }

            return result;
        }

        public IDataObject GetObjectByIndex(long index)
        {
            if (index < 0)
                return null;

            IDataObject result = null;
            try
            {
                Queue.ExecuteSync(
                (conn) =>
                {
                    String cmdText = String.Format("SELECT {0}.* FROM {0} {1} ORDER BY {2} LIMIT ?,1",
                        _dataProvider.TableName,
                        GetJoinClause(_dataProvider.TableName),
                        AppendOrderByID(SortDescriptorHelper.GetSQLClauseRepresentation(_sortDescriptions), _dataProvider.TableName));
                    using (SQLiteCommand cmd = new SQLiteCommand(conn))
                    {
                        cmd.CommandText = cmdText;

                        SQLiteParameter sqLiteParam = new SQLiteParameter();
                        sqLiteParam.Value = index;
                        cmd.Parameters.Add(sqLiteParam);

                        using (var sqlReader = cmd.ExecuteReader())
                        {
                            if (sqlReader.HasRows)
                            {
                                sqlReader.Read();
                                result = _dataProvider.CreateDataObjectFromSQLReader(sqlReader, false);
                            }
                        }
                    }
                });
            }
            catch (SQLiteException)
            {
            }

            return result;
        }

        public long GetIndexByObjectId(long objectId)
        {
            if (objectId <= 0)
                return -1;

            long result = -1;
            try
            {
                Queue.ExecuteSync(
                (conn) =>
                {
                    // SELECT
                    // (SELECT COUNT(*) FROM Person AS Person_1  WHERE Person_1.FullNameReversed <= Person.FullNameReversed ORDER BY  FullNameReversed ASC, BirthDate ASC) AS Position,
                    // (SELECT COUNT(*) FROM Person AS Person_1  WHERE Person_1.FullNameReversed == Person.FullNameReversed AND Person_1.BirthDate <= Person.BirthDate ORDER BY  FullNameReversed ASC, BirthDate ASC) AS Position2
                    // FROM Person  WHERE ID = 102 ORDER BY  FullNameReversed ASC, BirthDate ASC

                    // SELECT
                    // (SELECT COUNT(*) FROM Person AS Person_1 INNER JOIN TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9 ON Person_1.ID = TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9.ID WHERE  Person_1.FullNameReversed < Person.FullNameReversed ORDER BY  FullNameReversed ASC, BirthDate ASC) AS Position1,
                    // (SELECT COUNT(*) FROM Person AS Person_1 INNER JOIN TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9 ON Person_1.ID = TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9.ID WHERE Person_1.FullNameReversed == Person.FullNameReversed AND  Person_1.BirthDate < Person.BirthDate ORDER BY  FullNameReversed ASC, BirthDate ASC) AS Position2
                    // FROM Person INNER JOIN TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9 ON Person.ID = TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9.ID WHERE Person.ID = ? ORDER BY  FullNameReversed ASC, BirthDate ASC

                    String cmdText = GenerateFindIndexOfRowWithWhereClause(String.Format("{0}.ID = ?", _dataProvider.TableName), null);

                    using (SQLiteCommand cmd = new SQLiteCommand(conn))
                    {
                        cmd.CommandText = cmdText;

                        SQLiteParameter sqLiteParam = new SQLiteParameter();
                        sqLiteParam.Value = objectId;
                        cmd.Parameters.Add(sqLiteParam);

                        using (var sqlReader = cmd.ExecuteReader())
                        {
                            if (sqlReader.Read())
                            {
                                long? idx = null;
                                for (int i = 0; i < sqlReader.FieldCount; i++)
                                {
                                    if (!sqlReader.IsDBNull(i))
                                    {
                                        if (!idx.HasValue) idx = 0;
                                        idx += sqlReader.GetInt64(i);
                                    }
                                }
                                if (idx.HasValue)
                                    result = EnsureIndexOrIterateDownToFind(conn, idx.Value, objectId);// - positionCnt;   // take into account the presence operator '=' in condition on COUNTing
                            }
                        }
                    }
                });
            }
            catch (SQLiteException)
            {
            }
            catch (InvalidCastException)
            {
            }

            return result;
        }


        public long FindFirstIndex(string columnName, string predicateString)
        {
            if (columnName == null)
                throw new ArgumentNullException("columnName");

            long result = -1;
            try
            {
                Queue.ExecuteSync(
                (conn) =>
                {

                    String cmdText = GenerateFindIndexOfRowWithWhereClause(String.Format("{0}.{1} like '{2}'", _dataProvider.TableName, columnName, predicateString), "LIMIT 0, 1");

                    using (SQLiteCommand cmd = new SQLiteCommand(conn))
                    {
                        cmd.CommandText = cmdText;
                        
                        using (var sqlReader = cmd.ExecuteReader())
                        {
                            if (sqlReader.Read())
                            {
                                long? idx = null;
                                for (int i = 0; i < sqlReader.FieldCount; i++)
                                {
                                    if (!sqlReader.IsDBNull(i))
                                    {
                                        if (!idx.HasValue) idx = 0;
                                        idx += sqlReader.GetInt64(i);
                                    }
                                }
                                if (idx.HasValue)
                                    result = idx.Value;// - positionCnt;   // take into account the presence operator '=' in condition on COUNTing
                            }
                        }
                    }
                });
            }
            catch (SQLiteException)
            {
            }
            catch (InvalidCastException)
            {
            }

            return result;
        }

        public long FindNextIndex(string columnName, string predicateString, long index)
        {
            if (columnName == null)
                throw new ArgumentNullException("columnName");

            long result = -1;
            try
            {
                Queue.ExecuteSync(
                (conn) =>
                {
                    String cmdText = GenerateFindIndexOfRowWithWhereClause(
                        (positionColumns)=>
                        {
                            var sumClmns = String.Join("+", positionColumns);
                            return String.Format("{0}.{1} like '{2}' AND {3} >= ?",
                                _dataProvider.TableName,
                                columnName,
                                predicateString,
                                sumClmns);
                        }
                        , 
                        "LIMIT 1");

                    using (SQLiteCommand cmd = new SQLiteCommand(conn))
                    {
                        cmd.CommandText = cmdText;

                        SQLiteParameter sqLiteParam = new SQLiteParameter();
                        sqLiteParam.Value = index;
                        cmd.Parameters.Add(sqLiteParam);

                        using (var sqlReader = cmd.ExecuteReader())
                        {
                            if (sqlReader.Read())
                            {
                                long? idx = null;
                                for (int i = 0; i < sqlReader.FieldCount; i++)
                                {
                                    if (!sqlReader.IsDBNull(i))
                                    {
                                        if (!idx.HasValue) idx = 0;
                                        idx += sqlReader.GetInt64(i);
                                    }
                                }
                                if (idx.HasValue)
                                    result = idx.Value;// - positionCnt;   // take into account the presence operator '=' in condition on COUNTing
                            }
                        }
                    }
                });
            }
            catch (SQLiteException)
            {
            }
            catch (InvalidCastException)
            {
            }

            return result;
        }

        public long FindPrevIndex(string columnName, string predicateString, long index)
        {
            if (columnName == null)
                throw new ArgumentNullException("columnName");

            long result = -1;
            try
            {
                // SELECT MAX(R.Position1 + R.Position2) FROM(
                //    SELECT
                //    (SELECT COUNT(*) FROM Person AS Person_1  WHERE  Person_1.FullNameReversed < Person.FullNameReversed ORDER BY  FullNameReversed ASC, BirthDate ASC, Person_1.ID ASC) AS Position1,
                //    (SELECT COUNT(*) FROM Person AS Person_1  WHERE Person_1.FullNameReversed == Person.FullNameReversed AND  Person_1.BirthDate < Person.BirthDate ORDER BY  FullNameReversed ASC, BirthDate ASC, Person_1.ID ASC) AS Position2
                //   FROM Person  WHERE Person.FullNameReversed like '%lena%' AND Position1 + Position2 <= ? ORDER BY  FullNameReversed ASC, BirthDate ASC, Person.ID ASC ) As R

                Queue.ExecuteSync(
                (conn) =>
                {
                    String clauseForMax = null;
                    const String selectResultName = "R";
                    String cmdText = GenerateFindIndexOfRowWithWhereClause(
                        (positionColumns) =>
                        {
                            clauseForMax = positionColumns.Aggregate<String>((prev,current) => String.Format("{0} + {1}.{2}", prev, selectResultName, current));

                            var sumClmns = String.Join("+", positionColumns);
                            return String.Format("{0}.{1} like '{2}' AND {3} <= ?",
                                _dataProvider.TableName,
                                columnName,
                                predicateString,
                                sumClmns);
                        }
                        ,
                        null);

                    cmdText = String.Format("SELECT MAX({0}) FROM({1}) AS {2}", clauseForMax, cmdText, selectResultName);

                    using (SQLiteCommand cmd = new SQLiteCommand(conn))
                    {
                        cmd.CommandText = cmdText;

                        SQLiteParameter sqLiteParam = new SQLiteParameter();
                        sqLiteParam.Value = index;
                        cmd.Parameters.Add(sqLiteParam);

                        using (var sqlReader = cmd.ExecuteReader())
                        {
                            if (sqlReader.Read())
                            {
                                long? idx = null;
                                for (int i = 0; i < sqlReader.FieldCount; i++)
                                {
                                    if (!sqlReader.IsDBNull(i))
                                    {
                                        if (!idx.HasValue) idx = 0;
                                        idx += sqlReader.GetInt64(i);
                                    }
                                }
                                if (idx.HasValue)
                                    result = idx.Value;// - positionCnt;   // take into account the presence operator '=' in condition on COUNTing
                            }
                        }
                    }
                });
            }
            catch (SQLiteException)
            {
            }
            catch (InvalidCastException)
            {
            }

            return result;
        }
                

        #endregion
        

        #region ISortedFilteredDataProvider
        public IList<long> FilteredIds
        {
            get
            {
                List<long> result = new List<long>();

                if (_tempTableWithFilteredIDs != null)
                {
                    try
                    {
                        Queue.ExecuteSync((conn) =>
                        {
                            using (SQLiteCommand cmd = new SQLiteCommand(conn))
                            {
                                cmd.CommandText = "SELECT ID FROM " + _tempTableWithFilteredIDs;
                                using (var sqlReader = cmd.ExecuteReader())
                                {
                                    while (sqlReader.Read())
                                    {
                                        result.Add(sqlReader.GetInt64(0));
                                    }
                                }
                            }
                        });
                    }
                    catch (SQLiteException)
                    {
                        throw;
                    }
                }

                return result;
            }

            set
            {
                IList<long> newValue = value;
                if (newValue != null && newValue.Count == 0)
                    newValue = null;

                if (_tempTableWithFilteredIDs != null && newValue != null)
                {
                    // delete existed
                    try
                    {
                        Queue.ExecuteSync((conn) =>
                        {
                            using (SQLiteCommand cmd = new SQLiteCommand(conn))
                            {
                                cmd.CommandText = "DELETE FROM " + _tempTableWithFilteredIDs;
                                cmd.ExecuteNonQuery();
                            }
                        });
                    }
                    catch (SQLiteException)
                    {
                        throw;
                    }
                }
                else if (_tempTableWithFilteredIDs == null && newValue != null)
                {
                    // create table
                    _tempTableWithFilteredIDs = "TEMP_FOR_INDEX_" + Guid.NewGuid().ToString("N");
                    try
                    {
                        Queue.ExecuteSync((conn) =>
                        {
                            using (SQLiteCommand cmd = new SQLiteCommand(conn))
                            {
                                cmd.CommandText = String.Format("CREATE TEMPORARY TABLE IF NOT EXISTS {0} (ID INTEGER)", _tempTableWithFilteredIDs);
                                cmd.ExecuteNonQuery();
                            }
                        });
                    }
                    catch (SQLiteException)
                    {
                        throw;
                    }
                }
                else if (_tempTableWithFilteredIDs != null && newValue == null)
                {
                    // drop table                    
                    try
                    {
                        Queue.ExecuteSync((conn) =>
                        {
                            using (SQLiteCommand cmd = new SQLiteCommand(conn))
                            {
                                cmd.CommandText = "DROP TABLE IF EXISTS " + _tempTableWithFilteredIDs;
                                cmd.ExecuteNonQuery();
                            }
                        });
                    }
                    catch (SQLiteException)
                    {
                        throw;
                    }

                    _tempTableWithFilteredIDs = null;
                }


                if (_tempTableWithFilteredIDs != null && newValue != null)
                {
                    // fill table
                    try
                    {
                        Queue.ExecuteSync((conn) =>
                        {
                            using (SQLiteCommand cmd10 = new SQLiteCommand(conn))
                            using (SQLiteCommand cmd1 = new SQLiteCommand(conn))
                            {
                                cmd10.CommandText = String.Format("INSERT INTO {0} (ID) VALUES(?),(?),(?),(?),(?),(?),(?),(?),(?),(?)", _tempTableWithFilteredIDs);
                                cmd1.CommandText = String.Format("INSERT INTO {0} (ID) VALUES(?)", _tempTableWithFilteredIDs);

                                int cnt = newValue.Count;
                                while (cnt > 0)
                                {
                                    SQLiteCommand cmd2Execute = null;
                                    if (cnt >= 10)
                                    {
                                        cmd10.Parameters.Clear();
                                        for (int i = 0; i < 10; ++i)
                                        {
                                            --cnt;
                                            var sqLiteParam = new SQLiteParameter();
                                            sqLiteParam.Value = newValue[cnt];
                                            cmd10.Parameters.Add(sqLiteParam);
                                        }
                                        cmd2Execute = cmd10;
                                    }
                                    else
                                    {
                                        cmd1.Parameters.Clear();
                                        --cnt;
                                        var sqLiteParam = new SQLiteParameter();
                                        sqLiteParam.Value = newValue[cnt];
                                        cmd1.Parameters.Add(sqLiteParam);

                                        cmd2Execute = cmd1;
                                    }

                                    cmd2Execute.ExecuteNonQuery();
                                }
                            }
                        });
                    }
                    catch (SQLiteException)
                    {
                        throw;
                    }
                }
            }
        }
        #endregion


        #region IDisposable Support
        private bool _disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    Task t = Queue.ExecuteAsync(
                    (conn) =>
                    {
                        ((ISlimDataObjectProvider)_dataProvider).StopUsingIndex(conn, _sortDescriptions);
                    });
                }

                _disposedValue = true;
            }
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion


        IDatabaseQueue Queue { get { return ((ISlimDataObjectProvider)_dataProvider).LongOperationQueue; } }

        private string GetJoinClause(string tableNameToJoin)
        {
            string result = null;

            if (_tempTableWithFilteredIDs != null)
                result = String.Format("INNER JOIN {0} ON {1}.ID = {0}.ID", _tempTableWithFilteredIDs, tableNameToJoin);

            return result;
        }

        private IList<string> GenerateListOfCalculateOrdinalPositionWhereClause(IEnumerable<SortDescriptor> sortDescriptions, string leftTableName, string rightTableName)
        {
            List<string> whereClauses = new List<string>();

            List<string> equalFieldsList = new List<string>();
            foreach (var sd in sortDescriptions)
            {
                string equalsCompare = String.Join(" AND ", equalFieldsList);
                if (!String.IsNullOrWhiteSpace(equalsCompare))
                {
                    equalsCompare += " AND ";
                }

                var whereClause = String.Format("{0} {2}.{1} < {3}.{1}", equalsCompare, sd.ColumnName, leftTableName, rightTableName);
                whereClauses.Add(whereClause);

                var equalClause = String.Format("{1}.{0} == {2}.{0}", sd.ColumnName, leftTableName, rightTableName);
                equalFieldsList.Add(equalClause);
            }

            return whereClauses;
        }

        private long EnsureIndexOrIterateDownToFind(SQLiteConnection conn, long index, long id)
        {
            long cnt = Count;
            long idByIndex = -1;
            do
            {
                idByIndex = GetObjectIdByIndex(conn, index);
                ++index;
            }
            while (id != idByIndex && index < cnt);

            if (id == idByIndex)
                return index - 1;
            else
                return -1;
        }

        private String AppendOrderByID(string orderClause, string tableName)
        {
            return String.Format("{0}, {1}.ID ASC", orderClause, tableName);
        }

        private string GenerateFindIndexOfRowWithWhereClause(string whereClause, string limitClause = null)
        {
            return GenerateFindIndexOfRowWithWhereClause((paramColumnName) => whereClause, limitClause);
        }

        private string GenerateFindIndexOfRowWithWhereClause(Func<IList<string>, String> whereCreator, string limitClause = null)
        {
            // example in case  without filter
            // SELECT
            // (SELECT COUNT(*) FROM Person AS Person_1  WHERE Person_1.FullNameReversed <= Person.FullNameReversed ORDER BY  FullNameReversed ASC, BirthDate ASC, ID ASC) AS Position,
            // (SELECT COUNT(*) FROM Person AS Person_1  WHERE Person_1.FullNameReversed == Person.FullNameReversed AND Person_1.BirthDate <= Person.BirthDate ORDER BY  FullNameReversed ASC, BirthDate ASC, ID ASC) AS Position2
            // FROM Person  WHERE ID = 102 ORDER BY  FullNameReversed ASC, BirthDate ASC, ID ASC

            // example in case with filter
            // SELECT
            // (SELECT COUNT(*) FROM Person AS Person_1 INNER JOIN TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9 ON Person_1.ID = TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9.ID WHERE  Person_1.FullNameReversed < Person.FullNameReversed ORDER BY  FullNameReversed ASC, BirthDate ASC, ID ASC) AS Position1,
            // (SELECT COUNT(*) FROM Person AS Person_1 INNER JOIN TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9 ON Person_1.ID = TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9.ID WHERE Person_1.FullNameReversed == Person.FullNameReversed AND  Person_1.BirthDate < Person.BirthDate ORDER BY  FullNameReversed ASC, BirthDate ASC, ID ASC) AS Position2
            // FROM Person INNER JOIN TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9 ON Person.ID = TEMP_FOR_INDEX_0b092586e97c4bbdbb4eb689ddbe4ad9.ID WHERE Person.ID = ? ORDER BY  FullNameReversed ASC, BirthDate ASC, ID ASC

            string asTableName = _dataProvider.TableName + "_1";

            IList<string> positions = new List<string>();
            int positionCnt = 0;
            List<string> positionColumName = new List<string>(positionCnt);
            foreach (var posWhereClause in GenerateListOfCalculateOrdinalPositionWhereClause(_sortDescriptions, asTableName, _dataProvider.TableName))
            {
                ++positionCnt;
                String positionColumnName = String.Format("Position{0}", positionCnt);
                positionColumName.Add(positionColumnName);
                String cmdPosition = String.Format("(SELECT COUNT(*) FROM {0} AS {1} {2} WHERE {3} ORDER BY {4}) AS {5}",
                _dataProvider.TableName,
                asTableName,
                GetJoinClause(asTableName),
                posWhereClause,
                AppendOrderByID(SortDescriptorHelper.GetSQLClauseRepresentation(_sortDescriptions), asTableName),
                positionColumnName);
                positions.Add(cmdPosition);
            }            

            string positionSelects = String.Join(", ", positions);
            String cmdText = String.Format("SELECT {0} FROM {1} {2} WHERE {4} ORDER BY {3} {5}",
                positionSelects,
                _dataProvider.TableName,
                GetJoinClause(_dataProvider.TableName),
                AppendOrderByID(SortDescriptorHelper.GetSQLClauseRepresentation(_sortDescriptions), _dataProvider.TableName),
                whereCreator(positionColumName),
                limitClause);

            return cmdText;
        }

        public long FindFirstIndexRegexp(string columnName, string regexp)
        {
            throw new NotImplementedException("FindFirstIndexRegexp");
        }

        public long FindNextIndexRegexp(string columnName, string regexp, long index)
        {
            throw new NotImplementedException("FindNextIndexRegexp");
        }

        public long FindPrevIndexRegexp(string columnName, string regexp, long index)
        {
            throw new NotImplementedException("FindPrevIndexRegexp");
        }

    }
}
