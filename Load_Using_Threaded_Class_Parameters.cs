using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using Excel;
using System.IO;
using System.Threading;

namespace LoadExcelToDB
{
    public class WorkSheetParameter
    {
        public string workSheetName;
        public int id;
        public int skipsLine;
        public bool includeHeader;
    }

    public class FileParameter
    {
        public string fileLocation;
        public string fileName;
        public WorkSheetParameter[] workSheets;
    }

    class LoadExcelToDB
    {
        private static SqlConnection connection;
        private static string tableName = "extracts";
        private static string detailsTableName = "extract_details";
        private static Semaphore accessExcel = new Semaphore(1, 1);

        static void Main(string[] args)
        {
            openConnection();

            SqlCommand command = new SqlCommand("SELECT distinct fileLocation, fileName FROM dbo." + tableName, connection);
            SqlDataReader dataReader = command.ExecuteReader();

            List<Thread> threads = new List<Thread>();

            if (dataReader.HasRows)
            {
                while (dataReader.Read())
                {
                    try
                    {
                        Thread newThread = new Thread(new ParameterizedThreadStart(loadFile));

                        FileParameter _f = new FileParameter {fileLocation = (string)dataReader["fileLocation"], fileName = (string)dataReader["fileName"]};
                        var x = String.Format("SELECT id, worksheetName, rowsSkipped, includeHeader FROM dbo.{0} where fileLocation = '{1}' and fileName = '{2}'", tableName, (string)dataReader["fileLocation"], (string)dataReader["fileName"]);
                  
                        using (SqlCommand _cmd = new SqlCommand(x, connection))
                        {
                            SqlDataReader _rows = _cmd.ExecuteReader();

                            if (_rows.HasRows)
                            {
                                List<WorkSheetParameter> _ws = new List<WorkSheetParameter>();
                                while (_rows.Read())
                                {
                                    _ws.Add(new WorkSheetParameter {workSheetName       = (string)_rows["worksheetName"],
                                                                    id                  = (int)_rows["id"],
                                                                    skipsLine           = (int)_rows["rowsSkipped"],
                                                                    includeHeader       = (bool)_rows["includeHeader"]});
                                }

                                _f.workSheets = _ws.ToArray();
                            }
                            else { }
                        }

                        threads.Add(newThread);
                        newThread.Start(_f);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error while opening the file: " + ex.ToString());
                        throw;
                    }
                }
            }
            else
            {
                Console.WriteLine("No rows found.");
            }

            dataReader.Close();

            //wait for all threads
            bool isAnyAlive = true;

            while(isAnyAlive)
            {
                Thread.Sleep(30 * 1000);

                isAnyAlive = false;

                foreach (var item in threads)
                    isAnyAlive |= item.IsAlive;
            }

            System.Console.WriteLine("Press any key ...");
            System.Console.ReadKey();

            connection.Close();
        }

        private static void openConnection()
        {
            var connetionString = "Data Source=localhost;Initial Catalog=<>;Trusted_Connection=Yes; MultipleActiveResultSets=True;";
            connection = new SqlConnection(connetionString);

            try
            {
                connection.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error while opening DB connection: " + ex.ToString());
                throw;
            }
        }

        private static IExcelDataReader openExtractFile(String fileName)
        {
            IExcelDataReader result = null;

            FileStream stream = System.IO.File.Open(fileName, FileMode.Open, FileAccess.Read);

            if (fileName.ToLower().EndsWith(".xls"))
                result = ExcelReaderFactory.CreateBinaryReader(stream);
            else
            {
                if (fileName.ToLower().EndsWith(".xlsx") || fileName.ToLower().EndsWith(".xlsm"))
                    result = ExcelReaderFactory.CreateOpenXmlReader(stream);
                else
                    throw new Exception("Error: The supplied file is not a .xls, .xlsx or .xlsm file");
            }

            return result;
        }

        private static Boolean isEmptyorNUll(object dr)
        {
            return ((dr == null) || string.IsNullOrEmpty((dr.ToString())));
        }

        private static void loadFile(Object parameters)
        {
            try
            {
                FileParameter _f = (FileParameter)parameters;
                string fileName = string.Format("{0}\\{1}",_f.fileLocation, _f.fileName);

                accessExcel.WaitOne();

                IExcelDataReader excelReader = openExtractFile(fileName);

                accessExcel.Release(1);

                Console.WriteLine(string.Format("Load of file {0} started...", fileName));

                var wb = excelReader.AsDataSet();

                foreach (WorkSheetParameter _ws in _f.workSheets)
                {
                        string workSheetName = _ws.workSheetName;
                        int rowsSkipped = _ws.skipsLine;
                        int id = _ws.id;
                        bool includeHeader = _ws.includeHeader;

                        var ws1 = wb.Tables[workSheetName];
                        var firstRow = rowsSkipped;

                        Console.WriteLine(String.Format("Work Sheet {0}...", workSheetName));

                        int i = 0;
                        int firstCol = -1;
                        while (firstCol < 0)
                        {
                            if (isEmptyorNUll(ws1.Rows[firstRow][i]))
                                i++;
                            else
                                firstCol = i;
                        }

                        SqlCommand command = new SqlCommand(String.Format("DELETE FROM {0} WHERE extractId = {1}", detailsTableName, id.ToString()), connection);
                        command.ExecuteNonQuery();

                        for (int _row = firstRow; ((_row < ws1.Rows.Count) && (!isEmptyorNUll(ws1.Rows[_row][0 + firstCol]))); _row++)
                        {
                            List<String> columns = new List<string>();
                            List<String> values = new List<string>();

                            for (int _col = -2; _col < (ws1.Columns.Count - firstCol); _col++)
                            {
                                switch (_col)
                                {
                                    case -2:
                                        columns.Add("extractId");
                                        values.Add(id.ToString());
                                        break;
                                    case -1:
                                        columns.Add("fileHeader");
                                        if ((_row == firstRow) && includeHeader)
                                            values.Add("1");
                                        else
                                            values.Add("0");
                                        break;
                                    default:
                                        columns.Add("col" + (_col + 1).ToString());
                                        values.Add
                                        (
                                            isEmptyorNUll(ws1.Rows[_row][_col + firstCol]) ?
                                            "''" :
                                            "'" + String.Format("{0}", ws1.Rows[_row][_col + firstCol]).Replace("'", "''") + "'"
                                        );
                                        break;
                                }
                            }

                            var sqlText1 = String.Format("insert into {0} ({1}) values({2})", detailsTableName, String.Join(",", columns.ToArray()), String.Join(",", values.ToArray()));
                            command = new SqlCommand(sqlText1, connection);

                            command.ExecuteNonQuery();
                        }
                }
                Console.WriteLine(String.Format("Load of file {0} ended...", fileName));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception: " + ex.ToString());
                throw;
            }
        }
    }
}
