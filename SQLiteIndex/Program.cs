using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLiteIndex
{
    class Program
    {
        static void DoOperationsWithIndexedTable(SQLiteConnection conn)
        {
            Stopwatch sw;
            Console.WriteLine("Create indexes...");
            sw = Stopwatch.StartNew();
            using (var cmd = conn.CreateCommand())
            {
                String cmdText1 = "DROP INDEX IF EXISTS Person_NameBirthDate";
                cmd.CommandText = cmdText1;
                cmd.ExecuteScalar();

                String cmdText = "CREATE INDEX Person_NameBirthDate ON Person (Name, BirthDate)";
                cmd.CommandText = cmdText;
                cmd.ExecuteScalar();
            }
            sw.Stop();
            Console.WriteLine("Creating indexes finished in: " + sw.ElapsedMilliseconds + " ms");

            Int64? personID = null;
            String personName = null;
            Console.WriteLine("Read 100 elements: from 100 to 200...");
            sw = Stopwatch.StartNew();
            using (var cmd = conn.CreateCommand())
            {
                String cmdText = "SELECT * FROM Person ORDER BY Name ASC, BirthDate ASC LIMIT ?,1";
                cmd.CommandText = cmdText;
                for (int i = 100; i < 201; ++i)
                {
                    cmd.Parameters.Clear();
                    SQLiteParameter param = new SQLiteParameter();
                    param.Value = i;
                    cmd.Parameters.Add(param);

                    using (var sqReader = cmd.ExecuteReader())
                    {
                        sqReader.Read();
                        Console.WriteLine(sqReader.GetString(3) + "\t" + sqReader.GetValue(7));
                        if (!personID.HasValue)
                        {
                            personID = sqReader.GetInt64(0);
                            personName = sqReader.GetString(3);
                        }
                    }
                }
            }
            sw.Stop();
            Console.WriteLine("Reading finished in: " + sw.ElapsedMilliseconds + " ms");

            Console.WriteLine("Update person with ID = " + personID.Value.ToString());
            sw = Stopwatch.StartNew();
            using (var cmd = conn.CreateCommand())
            {
                String cmdText = "UPDATE Person SET name = ? WHERE ID = ?";
                cmd.CommandText = cmdText;

                SQLiteParameter param = new SQLiteParameter();
                param.Value = personName + "1";
                cmd.Parameters.Add(param);
                param = new SQLiteParameter();
                param.Value = personID.Value;
                cmd.Parameters.Add(param);

                cmd.ExecuteScalar();
            }
            sw.Stop();
            Console.WriteLine("Updating finished in: " + sw.ElapsedMilliseconds + " ms");

            Console.WriteLine("Read 100 elements: from 100 to 200...");
            sw = Stopwatch.StartNew();
            using (var cmd = conn.CreateCommand())
            {
                String cmdText = "SELECT * FROM Person ORDER BY Name ASC, BirthDate ASC LIMIT ?,1";
                cmd.CommandText = cmdText;
                for (int i = 100; i < 201; ++i)
                {
                    cmd.Parameters.Clear();
                    SQLiteParameter param = new SQLiteParameter();
                    param.Value = i;
                    cmd.Parameters.Add(param);

                    using (var sqReader = cmd.ExecuteReader())
                    {
                        sqReader.Read();
                        Console.WriteLine(sqReader.GetString(3) + "\t" + sqReader.GetValue(7));
                        if (!personID.HasValue)
                        {
                            personID = sqReader.GetInt64(0);
                            personName = sqReader.GetString(3);
                        }
                    }
                }
            }
            sw.Stop();
            Console.WriteLine("Reading finished in: " + sw.ElapsedMilliseconds + " ms");

            using (var cmd = conn.CreateCommand())
            {
                String cmdText = "UPDATE Person SET name = ? WHERE ID = ?";
                cmd.CommandText = cmdText;

                SQLiteParameter param = new SQLiteParameter();
                param.Value = personName;
                cmd.Parameters.Add(param);
                param = new SQLiteParameter();
                param.Value = personID.Value;
                cmd.Parameters.Add(param);

                cmd.ExecuteScalar();
            }
           
        }

        static void DoOperationsWithTempTable(SQLiteConnection conn)
        {
            Stopwatch sw;
            Console.WriteLine("Create sorted temp table...");
            sw = Stopwatch.StartNew();
            using (var cmd = conn.CreateCommand())
            {
                String cmdText = "DROP TABLE IF EXISTS Person_TEMP";
                cmd.CommandText = cmdText;
                cmd.ExecuteScalar();

                String cmdText1 = "CREATE TEMP TABLE Person_TEMP AS SELECT * FROM Person ORDER BY Name ASC, BirthDate ASC";
                cmd.CommandText = cmdText1;
                cmd.ExecuteScalar();               
            }
            sw.Stop();
            Console.WriteLine("Creating temp table finished in: " + sw.ElapsedMilliseconds + " ms");

            Int64? personID = null;
            String personName = null;
            object birthDay = null;
            Int64 rowID = 0;
            Console.WriteLine("Read 100 elements: from 100 to 200...");
            sw = Stopwatch.StartNew();
            using (var cmd = conn.CreateCommand())
            {
                String cmdText = "SELECT * FROM Person_TEMP WHERE RowID = ?";
                cmd.CommandText = cmdText;
                for (int i = 100; i < 201; ++i)
                {
                    cmd.Parameters.Clear();
                    SQLiteParameter param = new SQLiteParameter();
                    param.Value = i;
                    cmd.Parameters.Add(param);

                    using (var sqReader = cmd.ExecuteReader())
                    {
                        sqReader.Read();
                        Console.WriteLine(i + "\t" + sqReader.GetString(3) + "\t" + sqReader.GetValue(7));
                        if (!personID.HasValue && DBNull.Value != sqReader.GetValue(7))
                        {
                            personID = sqReader.GetInt64(0);
                            personName = sqReader.GetString(3);
                            birthDay = sqReader.GetValue(7);
                            rowID = i;
                        }
                    }
                }
            }
            sw.Stop();
            Console.WriteLine("Reading finished in: " + sw.ElapsedMilliseconds + " ms");

            string newPersonName = personName + "1";
            Console.WriteLine("Change position of the edited item...");
            sw = Stopwatch.StartNew();
            using (var cmd = conn.CreateCommand())
            {

                String cmdText = "ALTER TABLE Person_TEMP ADD POSITION integer";
                cmd.CommandText = cmdText;
                cmd.ExecuteScalar();

                cmdText = "UPDATE  Person_TEMP SET POSITION = RowID";
                cmd.CommandText = cmdText;
                cmd.ExecuteScalar();

                cmdText = "SELECT RowID FROM Person_TEMP WHERE Name > ? AND (BirthDate IS NULL OR BirthDate > ?)";
                cmd.CommandText = cmdText;

                SQLiteParameter param = new SQLiteParameter();
                param.Value = newPersonName;
                cmd.Parameters.Add(param);
                param = new SQLiteParameter();
                param.Value = birthDay;
                cmd.Parameters.Add(param);

                var val = cmd.ExecuteScalar();


                //cmd.Parameters.Clear();
                //cmdText = "UPDATE  Person_TEMP SET POSITION = POSITION + 1 WHERE POSITION >= ?";
                //cmd.CommandText = cmdText;
                //param = new SQLiteParameter();
                //param.Value = val;
                //cmd.Parameters.Add(param);
                //cmd.ExecuteNonQuery();

                

                cmd.Parameters.Clear();
                cmdText = "UPDATE Person_TEMP SET POSITION = POSITION - 1 WHERE POSITION >= ? AND POSITION < ?";
                cmd.CommandText = cmdText;
                param = new SQLiteParameter();
                param.Value = rowID;
                cmd.Parameters.Add(param);
                param = new SQLiteParameter();
                param.Value = val;
                cmd.Parameters.Add(param);
                cmd.ExecuteNonQuery();

                cmd.Parameters.Clear();
                cmdText = "UPDATE  Person_TEMP SET POSITION = ? WHERE RowID = ?";
                cmd.CommandText = cmdText;
                param = new SQLiteParameter();
                param.Value = (Int64)val - 1;
                cmd.Parameters.Add(param);
                param = new SQLiteParameter();
                param.Value = rowID;
                cmd.Parameters.Add(param);
                cmd.ExecuteNonQuery();

                //using (var cmd2 = conn.CreateCommand())
                //{
                //    String cmdText2 = "SELECT * FROM Person_TEMP WHERE RowID = ?";
                //    cmd2.CommandText = cmdText2;
                //    for (int i = 100; i < 201; ++i)
                //    {
                //        cmd2.Parameters.Clear();
                //        SQLiteParameter param2 = new SQLiteParameter();
                //        param2.Value = i;
                //        cmd2.Parameters.Add(param2);

                //        using (var sqReader = cmd2.ExecuteReader())
                //        {
                //            sqReader.Read();
                //            Console.WriteLine(i + "\t" + sqReader.GetString(3) + "\t" + sqReader.GetValue(7) + "\t" + sqReader.GetValue(26));                           
                //        }
                //    }
                //}

                //cmdText = "UPDATE  Person_TEMP SET RowID = POSITION";
                //cmd.CommandText = cmdText;
                //cmd.ExecuteScalar();
            }
            sw.Stop();
            Console.WriteLine("Changing finished in: " + sw.ElapsedMilliseconds + " ms");

            Console.WriteLine("Read 100 elements: from 100 to 200...");
            sw = Stopwatch.StartNew();
            using (var cmd = conn.CreateCommand())
            {
                String cmdText = "SELECT * FROM Person_TEMP WHERE RowID = ?";
                cmd.CommandText = cmdText;
                for (int i = 100; i < 201; ++i)
                {
                    cmd.Parameters.Clear();
                    SQLiteParameter param = new SQLiteParameter();
                    param.Value = i;
                    cmd.Parameters.Add(param);

                    using (var sqReader = cmd.ExecuteReader())
                    {
                        sqReader.Read();
                        Console.WriteLine(sqReader.GetString(3) + "\t" + sqReader.GetValue(7));                        
                    }
                }
            }
            sw.Stop();
            Console.WriteLine("Reading finished in: " + sw.ElapsedMilliseconds + " ms");
        }



        static void Main(string[] args)
        {
            //using (SQLiteConnection conn = new SQLiteConnection(@"Data Source=D:\_AndroidPhone\_FromPhoneCard\FTM_Files\33k_ppl_b404-Coverted-Copy.ftm;"))
            //{
            //    conn.Open();

            //    DoOperationsWithIndexedTable(conn);

            //    Console.WriteLine("----------------------------------------------------------------------");
            //    Console.WriteLine("----------------------------------------------------------------------");
            //    Console.WriteLine("----------------------------------------------------------------------");

            //    DoOperationsWithIndexedTable(conn);
            //}

            using (SQLiteConnection conn = new SQLiteConnection(@"Data Source=D:\_AndroidPhone\_FromPhoneCard\FTM_Files\33k_ppl_b404-Coverted-Copy2.ftm;"))
            {
                conn.Open();

                DoOperationsWithTempTable(conn);

                Console.WriteLine("----------------------------------------------------------------------");
                Console.WriteLine("----------------------------------------------------------------------");
                Console.WriteLine("----------------------------------------------------------------------");

                DoOperationsWithTempTable(conn);
            }
        }
    }
}
