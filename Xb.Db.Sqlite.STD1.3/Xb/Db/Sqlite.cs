using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace Xb.Db
{
    /// <summary>
    /// SQLite用DB接続管理クラス
    /// </summary>
    /// <remarks>
    /// Xb.Dbクラスを継承して実装。
    /// 主な処理が親クラスに書いてあり、ロジックが行き来するので注意。
    /// </remarks>
    public class Sqlite : Xb.Db.DbBase
    {
        /// <summary>
        /// Database-file name
        /// DBファイル名
        /// </summary>
        public string FileName => this.Address;

        /// <summary>
        /// Encoding(hide property)
        /// </summary>
        private new Encoding Encoding { get; set; }

        /// <summary>
        /// Constructor
        /// コンストラクタ
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="additionalString"></param>
        /// <param name="isBuildModels"></param>
        /// <remarks></remarks>
        public Sqlite(string fileName
                    , string additionalString = ""
                    , bool isBuildModels = true)
            : base(System.IO.Path.GetFileName(fileName)
                 , ""
                 , ""
                 , fileName
                 , additionalString
                 , isBuildModels)
        {
            this.Init();
        }

        /// <summary>
        /// Constructor
        /// コンストラクタ
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="isBuildModels"></param>
        public Sqlite(SqliteConnection connection
                    , bool isBuildModels = true)
            : base (connection
                  , ""
                  , isBuildModels)
        {
            this.Init();
        }


        private void Init()
        {
            this.Encoding = Encoding.UTF8;
            this.SqlFind = "SELECT * FROM {0} WHERE {1} LIMIT 1 ";
        }


        /// <summary>
        /// Connect DB
        /// DBへ接続する
        /// </summary>>
        /// <remarks></remarks>
        protected override void Open()
        {
            if(string.IsNullOrEmpty(this.Address))
                throw new InvalidOperationException("Xb.Db.Sqlite.Open: Db-file path not set.");
            
            //build connection string
            var connectionString 
                = string.Format("Data Source={0}{1}",
                                this.Address,
                                string.IsNullOrEmpty(this.AdditionalConnectionString)
                                    ? ""
                                    : "; " + this.AdditionalConnectionString);

            //allowed options: DataSource, Mode, Cache
            //https://github.com/aspnet/Microsoft.Data.Sqlite/wiki/Connection-Strings

            try
            {
                //connect DB
                this.Connection = new SqliteConnection(connectionString);
                this.Connection.Open();
            }
            catch (Exception ex)
            {
                this.Connection = null;
                Xb.Util.Out(ex);
                throw ex;
            }

            //init transaction
            this.ResetTransaction(false);
        }


        /// <summary>
        /// Get Table-Structure
        /// 接続先DBの構造を取得する。
        /// </summary>
        /// <remarks></remarks>
        protected override void GetStructure()
        {
            //get Table list
            var sql = new System.Text.StringBuilder();
            sql.AppendFormat(" SELECT ");
            sql.AppendFormat("     name as TABLE_NAME ");
            sql.AppendFormat(" FROM ");
            sql.AppendFormat("     sqlite_master ");
            sql.AppendFormat(" WHERE ");
            sql.AppendFormat("     type='table' ");
            sql.AppendFormat("     and name<>'sqlite_sequence' ");
            var dt = this.Query(sql.ToString());
            this.TableNames = dt.Rows.Select(row => row["TABLE_NAME"].ToString()).ToArray();

            var structRows = new List<Xb.Db.DbBase.Structure>();


            //テーブル一つずつ、順次カラム情報を追加していく。
            foreach (string tableName in this.TableNames)
            {
                //カラム情報を取得する。
                sql.Clear();
                sql.AppendFormat("PRAGMA table_info('{0}') ", tableName);
                var dt2 = this.Query(sql.ToString());
                if (dt2 == null
                    || dt2.Rows.Count <= 0)
                {
                    Xb.Util.Out("Xb.Db.Sqlite.GetStructure: カラム情報の取得に失敗しました。");
                    throw new Exception("Xb.Db.Sqlite.GetStructure: カラム情報の取得に失敗しました。");
                }

                foreach (ResultRow rrow in dt2.Rows)
                {
                    var row = new Xb.Db.DbBase.Structure();
                    var typeString = rrow["type"].ToString().ToLower();
                    var isNumber = false;
                    if (typeString.IndexOf("integer", StringComparison.Ordinal) != -1)
                    {
                        typeString = "integer";
                        isNumber = true;

                        //数値型のとき
                        row.CHAR_LENGTH = -1;
                        row.NUM_PREC = 12;
                        row.NUM_SCALE = 0;
                    }
                    else if (typeString.IndexOf("real", StringComparison.Ordinal) != -1)
                    {
                        typeString = "real";
                        isNumber = true;

                        //数値型のとき
                        row.CHAR_LENGTH = -1;
                        row.NUM_PREC = 12;
                        row.NUM_SCALE = 6;
                    }
                    else if (typeString.IndexOf("numeric", StringComparison.Ordinal) != -1)
                    {
                        typeString = "numeric";
                        isNumber = true;

                        //数値型のとき
                        row.CHAR_LENGTH = -1;
                        row.NUM_PREC = 12;
                        row.NUM_SCALE = 6;
                    }
                    else if (typeString.IndexOf("text", StringComparison.Ordinal) != -1)
                    {
                        typeString = "text";
                    }
                    else if (typeString.IndexOf("blob", StringComparison.Ordinal) != -1)
                    {
                        typeString = "blob";
                    }
                    else if (typeString.IndexOf("varchar", StringComparison.Ordinal) != -1)
                    {
                        typeString = "varchar";
                    }

                    else if (typeString.IndexOf("datetime", StringComparison.Ordinal) != -1)
                    {
                        typeString = "datetime";
                    }
                    else if (typeString.IndexOf("boolean", StringComparison.Ordinal) != -1)
                    {
                        typeString = "boolean";
                    }
                    else if (typeString.IndexOf("none", StringComparison.Ordinal) != -1)
                    {
                        typeString = "none";
                    }
                    else
                    {
                        //typeString = typeString;
                    }

                    row.TABLE_NAME = tableName;
                    row.COLUMN_INDEX = (long)rrow["cid"];
                    row.COLUMN_NAME  = rrow["name"].ToString();
                    row.TYPE = typeString;
                    row.IS_PRIMARY_KEY = (long)(((long)rrow["pk"] > 0) ? 1 : 0);
                    row.IS_NULLABLE = Convert.ToInt32(rrow["notnull"]) == 0
                        ? 1
                        : 0;
                    row.COMMENT = "";

                    if (!isNumber)
                    {
                        //文字型、その他のとき
                        row.CHAR_LENGTH = 65335;
                        row.NUM_PREC = -1;
                        row.NUM_SCALE = -1;
                    }

                    structRows.Add(row);
                }
            }

            this.StructureTable = structRows.ToArray();
        }


        /// <summary>
        /// Get DbParameter object.
        /// DbParameterオブジェクトを取得する。
        /// </summary>
        /// <returns></returns>
        public DbParameter GetParameter(string name = null
                                       , object value = null
                                       , SqliteType type = SqliteType.Text)
        {
            if (!string.IsNullOrEmpty(name)
                && name.Substring(0, 1) != "@")
                name = "@" + name;

            var param = new SqliteParameter();
            param.Direction = ParameterDirection.Input;

            //param.ParameterName = name ?? "";
            if (!string.IsNullOrEmpty(name))
                param.ParameterName = name;

            param.Value = value;
            param.SqliteType = type;
            //param.DbType = type;

            return param;
        }


        /// <summary>
        /// Get DbCommand object.
        /// DbCommandオブジェクトを取得する。
        /// </summary>
        /// <param name="parameters"></param>
        /// <returns></returns>
        protected override DbCommand GetCommand(DbParameter[] parameters = null)
        {
            var result = new SqliteCommand
            {
                Connection = (SqliteConnection)this.Connection
            };

            if (parameters != null
                && parameters.Length > 0)
            {
                //result.Parameters.AddRange(parameters);
                foreach (var parameter in parameters)
                    result.Parameters.Add((SqliteParameter)parameter);
            }

            return result;
        }


        /// <summary>
        /// Get Database backup file
        /// データベースのバックアップファイルを取得する。
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public override async Task<bool> BackupDbAsync(string fileName)
        {
            //渡し値パスが実在することを確認する。
            if (!await this.RemoveIfExintsAsync(fileName))
                return false;

            //DBバックアップを実行する。
            try
            {
                //DBファイルをコピーする。
                System.IO.File.Copy(this.Name, fileName, true);
            }
            catch (Exception ex)
            {
                Xb.Util.Out("Xb.Db.Sqlite.BackupDb: バックアップファイル取得に失敗しました：" + ex.Message);
                throw new IOException("Xb.Db.Sqlite.BackupDb: バックアップファイル取得に失敗しました：" + ex.Message);
            }

            return true;
        }
    }
}