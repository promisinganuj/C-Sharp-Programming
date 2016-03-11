using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using Excel;
using System.IO;

namespace LoadExcelToDB
{
    class LoadExcelToDB
    {
        private static SqlConnection connection;

        private static string tableName         = "ap_extracts";
        private static string detailsTableName  = "ap_extract_details";

        static void Main(string[] args)
        {
            openConnection();

            SqlCommand command = new SqlCommand("SELECT * FROM dbo." + tableName, connection);
            SqlDataReader dataReader = command.ExecuteReader();

            if (dataReader.HasRows)
            {
                while (dataReader.Read())
                {
                    try
                    {
                        loadFile
                        (
                            (string)dataReader["fileLocation"] + '\\' + (string)dataReader["fileName"],
                            (string)dataReader["worksheetName"],
                            (int)dataReader["rowsSkipped"],
                            (int)dataReader["id"],
                            (bool)dataReader["includeHeader"]
                        );
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
            return ((dr == null) || String.IsNullOrEmpty((dr.ToString())));
        }

        private static void loadFile(String fileName, String worksheetName, int rowsSkipped, int id, Boolean includeHeader)
        {
            try
            {
                IExcelDataReader excelReader = openExtractFile(fileName);

                var wb = excelReader.AsDataSet();
                var ws1 = wb.Tables[worksheetName];
                var firstRow = rowsSkipped;

                int i = 0;
                int firstCol = -1;
                while (firstCol < 0)
                {
                    if (isEmptyorNUll(ws1.Rows[firstRow][i]))
                        i++;
                    else
                        firstCol = i;
                }

                SqlCommand command = new SqlCommand("DELETE FROM " + detailsTableName + " WHERE extractId = " + id.ToString(), connection);
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
            catch (Exception ex)
            {
                Console.WriteLine("Exception: " + ex.ToString());
                throw;
            }
        }
    }
}
