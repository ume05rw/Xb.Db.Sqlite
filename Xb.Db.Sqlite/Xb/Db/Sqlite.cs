using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;

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

        private readonly string _fileName;
        private new SQLiteConnection Connection;
        private new string SqlFind = "SELECT * FROM {0} WHERE {1} LIMIT 1 ";


        /// <summary>
        /// DBファイル名
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string FileName => this._fileName;


        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="password"></param>
        /// <param name="templateFileName"></param>
        /// <param name="isBuildStructureModels"></param>
        /// <param name="additionalString"></param>
        /// <remarks></remarks>
        public Sqlite(string fileName,
                      string password = null,
                      string templateFileName = null,
                      bool isBuildStructureModels = true,
                      string additionalString = "")
        {
            if (!System.IO.File.Exists(fileName)
                && System.IO.File.Exists(templateFileName))
            {
                System.IO.File.Copy(templateFileName, fileName);
            }

            if (password == null)
                password = "";

            //渡し値の接続用パラメータを保持する。
            this._fileName = fileName;
            this._password = password;
            this._additionalConnectionString = additionalString;
            base._encoding = System.Text.Encoding.UTF8;

            //DBへ接続する。
            this.Open();

            if (isBuildStructureModels)
            {
                //接続したDBの構造データを取得する。
                this.GetStructure();
            }

            //基底クラス側の値を上書きする。
            base.SqlFind = this.SqlFind;
        }


        /// <summary>
        /// DBへ接続する
        /// </summary>>
        /// <remarks></remarks>
        protected override void Open()
        {
            //コネクション変数の状態チェック-既に接続済みのとき、何もしない。
            if (this.IsConnected)
                return;

            //接続設定を文字列でセット
            var connectionString = string.Format("Version=3;Data Source={0};New=False;Compress=True;{1}",
                                                 this._fileName,
                                                 string.IsNullOrEmpty(this._additionalConnectionString)
                                                     ? ""
                                                     : "; " + this._additionalConnectionString);
            try
            {
                //接続
                this.Connection = new SQLiteConnection();
                this.Connection.ConnectionString = connectionString;
                this.Connection.Open();
                //this.SetConnection((System.Data.Common.DbConnection)this.Connection);

            }
            catch (Exception)
            {
                this.Connection = null;
            }

            if (!this.IsConnected)
            {
                Xb.Util.Out("Xb.Db.Sqlite.Open: DB接続に失敗しました。");
                throw new ApplicationException("Xb.Db.Sqlite.Open: DB接続に失敗しました。");
            }

            //トランザクションを初期化
            this._isInTransaction = false;
        }


        /// <summary>
        /// 接続先DBの構造を取得する。
        /// </summary>
        /// <remarks></remarks>
        protected override void GetStructure()
        {
            //テーブルリストを取得する。
            var sql = new System.Text.StringBuilder();
            sql.AppendFormat("\r\n SELECT ");
            sql.AppendFormat("\r\n     name as TABLE_NAME ");
            sql.AppendFormat("\r\n FROM sqlite_master ");
            sql.AppendFormat("\r\n WHERE type='table' and name<>'sqlite_sequence' ");
            var dt = this.Query(sql.ToString());

            this._tableNames = new List<string>();
            foreach (DataRow row in dt.Rows)
            {
                this._tableNames.Add(row["TABLE_NAME"].ToString());
            }

            //カラム情報を保持するDataTableを生成する。
            var structure = new DataTable();
            structure.Columns.Add("TABLE_NAME", typeof(Str));
            structure.Columns.Add("COLUMN_INDEX", typeof(Int32));
            structure.Columns.Add("COLUMN_NAME", typeof(Str));
            structure.Columns.Add("TYPE", typeof(Str));
            structure.Columns.Add("CHAR_LENGTH", typeof(Int32));
            structure.Columns.Add("NUM_PREC", typeof(Int32));
            structure.Columns.Add("NUM_SCALE", typeof(Int32));
            structure.Columns.Add("IS_PRIMARY_KEY", typeof(Int32));
            structure.Columns.Add("IS_NULLABLE", typeof(Int32));
            structure.Columns.Add("COMMENT", typeof(Str));

            //テーブル一つずつ、順次カラム情報を追加していく。
            foreach (string tableName in this._tableNames)
            {
                //カラム情報を取得する。
                sql = new System.Text.StringBuilder();
                sql.AppendFormat("PRAGMA table_info('{0}') ", tableName);
                var dt2 = this.Query(sql.ToString());
                if (dt2 == null
                    || dt2.Rows.Count <= 0)
                {
                    Xb.Util.Out("Xb.Db.Sqlite.GetStructure: カラム情報の取得に失敗しました。");
                    throw new ApplicationException("Xb.Db.Sqlite.GetStructure: カラム情報の取得に失敗しました。");
                }

                for (int i = 1; i <= dt2.Rows.Count - 1; i++)
                {
                    var row = structure.NewRow();
                    var typeString = dt2.Rows[i]["type"].ToString().ToLower();
                    var isNumber = false;
                    if (typeString.IndexOf("integer", StringComparison.Ordinal) != -1)
                    {
                        typeString = "integer";
                        isNumber = true;
                    }
                    else if (typeString.IndexOf("real", StringComparison.Ordinal) != -1)
                    {
                        typeString = "real";
                        isNumber = true;
                    }
                    else if (typeString.IndexOf("varchar", StringComparison.Ordinal) != -1)
                    {
                        typeString = "varchar";
                    }
                    else if (typeString.IndexOf("blob", StringComparison.Ordinal) != -1)
                    {
                        typeString = "blob";
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

                    row["TABLE_NAME"] = tableName;
                    row["COLUMN_INDEX"] = dt2.Rows[i]["cid"];
                    row["COLUMN_NAME"] = dt2.Rows[i]["name"];
                    row["TYPE"] = typeString;
                    row["IS_PRIMARY_KEY"] = dt2.Rows[i]["pk"];
                    row["IS_NULLABLE"] = Convert.ToInt32(dt2.Rows[i]["notnull"]) == 0
                                             ? 1
                                             : 0;
                    row["COMMENT"] = "";

                    if (isNumber)
                    {
                        //数値型のとき
                        row["CHAR_LENGTH"] = DBNull.Value;
                        row["NUM_PREC"] = 12;
                        row["NUM_SCALE"] = 6;
                    }
                    else
                    {
                        //文字型、その他のとき
                        row["CHAR_LENGTH"] = 65335;
                        row["NUM_PREC"] = DBNull.Value;
                        row["NUM_SCALE"] = DBNull.Value;
                    }

                    structure.Rows.Add(row);
                }
            }

            this._structureTable = structure;

            //テーブルごとのモデルインスタンスを生成・保持しておく。
            this.BuildModels();
        }


        /// <summary>
        /// SQL文でコマンドを実行する(結果を返さない)
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public override int Execute(string sql)
        {
            base.Command = new SQLiteCommand(sql, this.Connection);
            return base.Execute(sql);
        }


        /// <summary>
        /// SQL文でクエリを実行し、結果を返す
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public override DataTable Query(string sql)
        {
            base.Adapter = new SQLiteDataAdapter(sql, this.Connection);
            return base.Query(sql);
        }


        /// <summary>
        /// SQL文でクエリを実行し、結果を返す
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="dt"></param>
        /// <remarks></remarks>
        public override void Fill(string sql, DataTable dt)
        {
            base.Adapter = new SQLiteDataAdapter(sql, this.Connection);
            base.Fill(sql, dt);
        }


        /// <summary>
        /// データベースのバックアップファイルを取得する。
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public override bool BackupDb(string fileName)
        {
            //渡し値パスが実在することを確認する。
            if (!base.RemoveIfExints(fileName))
                return false;

            //DBバックアップを実行する。
            try
            {
                //DBファイルをコピーする。
                System.IO.File.Copy(this._fileName, fileName, true);
            }
            catch (Exception ex)
            {
                Xb.Util.Out("Xb.Db.Sqlite.BackupDb: バックアップファイル取得に失敗しました：" + ex.Message);
                throw new ApplicationException("Xb.Db.Sqlite.BackupDb: バックアップファイル取得に失敗しました：" + ex.Message);
            }

            return true;
        }
    }
}