using System;
using System.Text;
using System.Collections.Generic;
using System.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Data.Sqlite;


namespace TestXb.Db
{
    public class SqliteBase : TestXb.TestBase, IDisposable
    {
        protected Xb.Db.Sqlite _dbDirect;
        protected Xb.Db.Sqlite _dbRef;

        protected const string FileName = "sqlite_test.db3";
        protected SqliteConnection Connection;

        public SqliteBase(bool isBuildModel)
        {
            this.Out("SqliteBase.Constructor Start.");

            //remove if exists.
            if(System.IO.File.Exists(FileName))
                System.IO.File.Delete(FileName);


            this.Connection = new SqliteConnection();
            this.Connection.ConnectionString
                = $"Data Source={FileName}";

            //allowed options: DataSource, Mode, Cache
            //https://github.com/aspnet/Microsoft.Data.Sqlite/wiki/Connection-Strings

            try
            {
                this.Connection.Open();
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw ex;
            }

            var sql = " CREATE TABLE test ( "
                + "     COL_STR TEXT NOT NULL, "
                + "     COL_DEC REAL, "
                + "     COL_INT INTEGER, "
                + "     COL_DATETIME TEXT "
                + " ) ";
            this.Exec(sql);

            sql = " CREATE TABLE test2 ( "
                + "     COL_STR TEXT NOT NULL, "
                + "     COL_DEC REAL, "
                + "     COL_INT INTEGER, "
                + "     COL_DATETIME TEXT, "
                + "     PRIMARY KEY (COL_STR) "
                + " ) ";
            this.Exec(sql);

            sql = " CREATE TABLE test3 ( "
                + "     COL_STR TEXT NOT NULL, "
                + "     COL_DEC REAL, "
                + "     COL_INT INTEGER NOT NULL, "
                + "     COL_DATETIME TEXT, "
                + "     PRIMARY KEY (COL_STR, COL_INT) "
                + " ) ";
            this.Exec(sql);


            this.InitTables();

            try
            {
                this._dbDirect = new Xb.Db.Sqlite(SqliteBase.FileName
                                                , null
                                                , ""
                                                , isBuildModel);

                this._dbRef = new Xb.Db.Sqlite(this.Connection
                                             , isBuildModel);
            }
            catch (Exception ex)
            {
                Xb.Util.Out(ex);
                throw ex;
            }

            this.Out("SqliteBase.Constructor End.");
        }

        protected void InitTables(bool isSetData = true)
        {
            this.Exec("DELETE FROM test");
            this.Exec("DELETE FROM test2");
            this.Exec("DELETE FROM test3");

            if (!isSetData)
                return;

            var insertTpl = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME) VALUES ({1}, {2}, {3}, {4});";
            this.Exec(string.Format(insertTpl, "test", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test", "'BB'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test", "'CC'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test", "'KEY'", 0, "NULL", "'2000-12-31'"));

            this.Exec(string.Format(insertTpl, "test2", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test2", "'BB'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test2", "'CC'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test2", "'KEY'", 0, "NULL", "'2000-12-31'"));

            this.Exec(string.Format(insertTpl, "test3", "'ABC'", 1, 1, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test3", "'ABC'", 1, 2, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test3", "'ABC'", 1, 3, "'2001-01-01'"));
            this.Exec(string.Format(insertTpl, "test3", "'BB'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test3", "'CC'", 12.345, 12345, "'2016-12-13'"));
            this.Exec(string.Format(insertTpl, "test3", "'KEY'", "NULL", 0, "'2000-12-31'"));
        }

        protected int Exec(string sql)
        {
            var command = new SqliteCommand(sql, this.Connection);
            var result = command.ExecuteNonQuery();
            command.Dispose();

            return result;
        }
        
        public override void Dispose()
        {
            this.Out("SqliteBase.Dispose Start.");

            this._dbDirect.Dispose();
            this._dbRef.Dispose();

            this.Connection.Close();
            this.Connection.Dispose();

            System.Threading.Thread.Sleep(1000);

            try
            {System.IO.File.Delete(FileName);}
            catch (Exception){}

            this.Out("SqliteBase.Dispose End.");

            base.Dispose();
        }
    }
}
