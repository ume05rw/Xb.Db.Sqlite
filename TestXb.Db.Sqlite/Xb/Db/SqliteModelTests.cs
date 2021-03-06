﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestXb.Db;
using Xb.Db;

namespace TestsXb
{
    [TestClass()]
    public class SqliteModelTests : SqliteBase, IDisposable
    {
        private Xb.Db.Model _testModel;
        private Xb.Db.Model _test2Model;
        private Xb.Db.Model _test3Model;

        public SqliteModelTests() : base(true)
        {
        }

        private void InitModel(bool isConnectDirect)
        {
            this._testModel = isConnectDirect
                                ? this._dbDirect.Models["test"]
                                : this._dbRef.Models["test"];

            this._test2Model = isConnectDirect
                                ? this._dbDirect.Models["test2"]
                                : this._dbRef.Models["test2"];

            this._test3Model = isConnectDirect
                                ? this._dbDirect.Models["test3"]
                                : this._dbRef.Models["test3"];
        }

        [TestMethod()]
        public void ConstructorTest()
        {
            this.Out("ConstructorTest Start.");

            for (var constructorType = 0; constructorType < 2; constructorType++)
            {
                this.InitModel(constructorType == 0);

                Assert.AreEqual("test", this._testModel.TableName);
                Assert.AreEqual(0, this._testModel.PkeyColumns.Length);
                Assert.AreEqual(4, this._testModel.Columns.Length);

                Assert.AreEqual("test2", this._test2Model.TableName);
                Assert.AreEqual(1, this._test2Model.PkeyColumns.Length);
                Assert.AreEqual("COL_STR", this._test2Model.PkeyColumns[0].Name);
                Assert.AreEqual(4, this._test2Model.Columns.Length);

                Assert.AreEqual("test3", this._test3Model.TableName);
                Assert.AreEqual(2, this._test3Model.PkeyColumns.Length);
                Assert.AreEqual("COL_STR", this._test3Model.PkeyColumns[0].Name);
                Assert.AreEqual("COL_INT", this._test3Model.PkeyColumns[1].Name);
                Assert.AreEqual(4, this._test3Model.Columns.Length);
            }

            this.Out("ConstructorTest End.");
        }

        [TestMethod()]
        public void GetColumnTest()
        {
            this.Out("GetColumnTest Start.");

            for (var constructorType = 0; constructorType < 2; constructorType++)
            {
                this.InitModel(constructorType == 0);

                var col = this._test2Model.GetColumn("COL_STR");
                Assert.AreEqual("COL_STR", col.Name);
                Assert.AreEqual(Xb.Db.Model.Column.ColumnType.String, col.Type);
                Assert.AreEqual(65335, col.MaxLength);
                Assert.IsFalse(col.IsNullable);
                Assert.IsTrue(col.IsPrimaryKey);

                col = this._testModel.GetColumn("COL_DEC");
                Assert.AreEqual(Xb.Db.Model.Column.ColumnType.Number, col.Type);
                Assert.AreEqual(6, col.MaxInteger);
                Assert.AreEqual(6, col.MaxDecimal);
                Assert.IsTrue(col.IsNullable);
                Assert.IsFalse(col.IsPrimaryKey);

                col = this._test2Model.GetColumn(0);
                Assert.AreEqual("COL_STR", col.Name);
                Assert.AreEqual(Xb.Db.Model.Column.ColumnType.String, col.Type);
                Assert.AreEqual(65335, col.MaxLength);
                Assert.IsFalse(col.IsNullable);
                Assert.IsTrue(col.IsPrimaryKey);

                col = this._testModel.GetColumn(1);
                Assert.AreEqual(Xb.Db.Model.Column.ColumnType.Number, col.Type);
                Assert.AreEqual(6, col.MaxInteger);
                Assert.AreEqual(6, col.MaxDecimal);
                Assert.IsTrue(col.IsNullable);
                Assert.IsFalse(col.IsPrimaryKey);
            }

            this.Out("GetColumnTest End.");
        }


        [TestMethod()]
        public async Task FindTest()
        {
            this.Out("FindTest Start.");

            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    this.InitTables();
                    this.InitModel(constructorType == 0);

                    var row = (asyncType == 0)
                        ? this._test2Model.Find("BB")
                        : await this._test2Model.FindAsync("BB");
                    Assert.AreEqual("BB", row["COL_STR"]);
                    Assert.AreEqual((double)12.345, row["COL_DEC"]);
                    Assert.AreEqual((long)12345, row["COL_INT"]);
                    Assert.AreEqual("2016-12-13", row["COL_DATETIME"]);

                    row = (asyncType == 0)
                        ? this._test2Model.Find("KEY")
                        : await this._test2Model.FindAsync("KEY");
                    Assert.AreEqual("KEY", row["COL_STR"]);
                    Assert.AreEqual((double)0, row["COL_DEC"]);
                    Assert.AreEqual(DBNull.Value, row["COL_INT"]);
                    Assert.AreEqual("2000-12-31", row["COL_DATETIME"]);

                    row = (asyncType == 0)
                        ? this._test3Model.Find("ABC", 2)
                        : await this._test3Model.FindAsync("ABC", 2);
                    Assert.AreEqual("ABC", row["COL_STR"]);
                    Assert.AreEqual((double)1, row["COL_DEC"]);
                    Assert.AreEqual((long)2, row["COL_INT"]);
                    Assert.AreEqual("2001-01-01", row["COL_DATETIME"]);

                    row = (asyncType == 0)
                        ? this._test3Model.Find("KEY", 0)
                        : await this._test3Model.FindAsync("KEY", 0);
                    Assert.AreEqual("KEY", row["COL_STR"]);
                    Assert.AreEqual(DBNull.Value, row["COL_DEC"]);
                    Assert.AreEqual((long)0, row["COL_INT"]);
                    Assert.AreEqual("2000-12-31", row["COL_DATETIME"]);
                }
            }
            this.Out("FindTest End.");
        }


        [TestMethod()]
        public async Task FindAllTest()
        {
            this.Out("FindAllTest Start.");

            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    this.InitTables();
                    this.InitModel(constructorType == 0);

                    var table = (asyncType == 0)
                        ? this._test3Model.FindAll()
                        : await this._test3Model.FindAllAsync();
                    Assert.AreEqual(6, table.Rows.Count);

                    table = (asyncType == 0)
                        ? this._test3Model.FindAll("COL_STR LIKE '%B%' ", "COL_INT DESC")
                        : await this._test3Model.FindAllAsync("COL_STR LIKE '%B%' ", "COL_INT DESC");
                    Assert.AreEqual(4, table.Rows.Count);

                    Assert.AreEqual("BB", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)12.345, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)12345, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2016-12-13", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("ABC", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)1, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)3, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("2001-01-01", table.Rows[1]["COL_DATETIME"]);

                    Assert.AreEqual("ABC", table.Rows[2]["COL_STR"]);
                    Assert.AreEqual((double)1, table.Rows[2]["COL_DEC"]);
                    Assert.AreEqual((long)2, table.Rows[2]["COL_INT"]);
                    Assert.AreEqual("2001-01-01", table.Rows[2]["COL_DATETIME"]);

                    Assert.AreEqual("ABC", table.Rows[3]["COL_STR"]);
                    Assert.AreEqual((double)1, table.Rows[3]["COL_DEC"]);
                    Assert.AreEqual((long)1, table.Rows[3]["COL_INT"]);
                    Assert.AreEqual("2001-01-01", table.Rows[3]["COL_DATETIME"]);
                }
            }
            this.Out("FindAllTest End.");
        }


        [TestMethod()]
        public void NewRowTest()
        {
            this.Out("NewRowTest Start.");

            for (var constructorType = 0; constructorType < 2; constructorType++)
            {
                this.InitTables();
                this.InitModel(constructorType == 0);

                var row = this._test3Model.NewRow();
                Assert.AreEqual(4, row.Table.ColumnNames.Length);
                Assert.AreEqual("COL_STR", row.Table.ColumnNames[0]);
                Assert.AreEqual("COL_DEC", row.Table.ColumnNames[1]);
                Assert.AreEqual("COL_INT", row.Table.ColumnNames[2]);
                Assert.AreEqual("COL_DATETIME", row.Table.ColumnNames[3]);
            }

            this.Out("NewRowTest End.");
        }

        [TestMethod()]
        public void ValidateTest()
        {
            this.Out("ValidateTest Start.");

            for (var constructorType = 0; constructorType < 2; constructorType++)
            {
                this.InitTables();
                this.InitModel(constructorType == 0);

                Assert.AreEqual("test", this._testModel.TableName);

                Assert.AreEqual(4, this._testModel.Columns.Length);

                var row = this._testModel.NewRow();
                row["COL_STR"] = "1234567890";
                row["COL_DEC"] = 12.345;
                row["COL_INT"] = 2147483647;
                row["COL_DATETIME"] = "2016-01-01 19:59:59";

                var errors = this._testModel.Validate(row);
                if (errors.Length > 0)
                {
                    foreach (var error in errors)
                    {
                        this.Out(error.Name + ": " + error.Message);
                    }
                    Assert.Fail("エラーの値の検証でエラーが発生した。");
                }

                row = this._testModel.NewRow();
                errors = this._testModel.Validate(row);
                Assert.AreEqual(1, errors.Length);

                var err = errors[0];
                Assert.AreEqual("COL_STR", err.Name);
                this.Out(err.Name + ": " + err.Message);

                this._testModel.Columns[0].MaxLength = 10;
                row = this._testModel.NewRow();
                row["COL_STR"] = "12345678901";
                errors = this._testModel.Validate(row);
                Assert.AreEqual(1, errors.Length);
                err = errors[0];
                Assert.AreEqual("COL_STR", err.Name);
                this.Out(err.Name + ": " + err.Message);

                (this._testModel.GetColumn("COL_DEC")).MaxDecimal = 3;
                row = this._testModel.NewRow();
                row["COL_STR"] = "NOT NULL";
                row["COL_DEC"] = 1.1234;
                errors = this._testModel.Validate(row);
                Assert.AreEqual(1, errors.Length);
                err = errors[0];
                Assert.AreEqual("COL_DEC", err.Name);
                this.Out(err.Name + ": " + err.Message);

                (this._testModel.GetColumn("COL_DEC")).MaxInteger = 2;
                row = this._testModel.NewRow();
                row["COL_STR"] = "NOT NULL";
                row["COL_DEC"] = 123.123;
                errors = this._testModel.Validate(row);
                Assert.AreEqual(1, errors.Length);
                err = errors[0];
                Assert.AreEqual("COL_DEC", err.Name);
                this.Out(err.Name + ": " + err.Message);

                (this._testModel.GetColumn("COL_INT")).MaxInteger = 10;
                row = this._testModel.NewRow();
                row["COL_STR"] = "NOT NULL";
                row["COL_INT"] = 21474836471;
                errors = this._testModel.Validate(row);
                Assert.AreEqual(1, errors.Length);
                err = errors[0];
                Assert.AreEqual("COL_INT", err.Name);
                this.Out(err.Name + ": " + err.Message);

                //SQLite hasn't DateTime Type.
                //row = this._testModel.NewRow();
                //row["COL_STR"] = "NOT NULL";
                //row["COL_DATETIME"] = "12/99/99";
                //errors = this._testModel.Validate(row);
                //Assert.AreEqual(1, errors.Length);
                //err = errors[0];
                //Assert.AreEqual("COL_DATETIME", err.Name);
                //this.Out(err.Name + ": " + err.Message);

                row = this._test3Model.NewRow();
                errors = this._test3Model.Validate(row);
                Assert.AreEqual(2, errors.Length);

                var errorColumns = errors.Select(col => col.Name).ToArray();
                Assert.IsTrue(errorColumns.Contains("COL_STR"));
                Assert.IsTrue(errorColumns.Contains("COL_INT"));
                this.Out(errors[0].Name + ": " + errors[0].Message + "  /  " + errors[1].Name + ": " + errors[1].Message);
            }
            this.Out("ValidateTest End.");
        }


        [TestMethod()]
        public async Task WriteAsyncTest()
        {
            this.Out("WriteAsyncTest Start.");

            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    this.InitTables(false);
                    this.InitModel(constructorType == 0);

                    var row = this._test2Model.NewRow();
                    row["COL_STR"] = "P001";
                    row["COL_DEC"] = 12.345;
                    row["COL_INT"] = 2147483647;
                    row["COL_DATETIME"] = "2016-01-01 19:59:59";

                    var errs = (asyncType == 0)
                        ? this._test2Model.Write(row)
                        : await this._test2Model.WriteAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 0;
                    row["COL_INT"] = 0;
                    row["COL_DATETIME"] = "2000-01-01";

                    errs = (asyncType == 0)
                        ? this._test2Model.Write(row)
                        : await this._test2Model.WriteAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    var table = (asyncType == 0)
                        ? this._test2Model.FindAll(null, "COL_STR")
                        : await this._test2Model.FindAllAsync("", "COL_STR");
                    Assert.AreEqual(2, table.Rows.Count);

                    Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)12.345, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)2147483647, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2016-01-01 19:59:59", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("2000-01-01", table.Rows[1]["COL_DATETIME"]);


                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 1;
                    row["COL_INT"] = 2;
                    row["COL_DATETIME"] = "2000-01-03";

                    errs = (asyncType == 0)
                        ? this._test2Model.Write(row)
                        : await this._test2Model.WriteAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                        ? this._test2Model.FindAll("", "COL_STR")
                        : await this._test2Model.FindAllAsync("", "COL_STR");

                    Assert.AreEqual(2, table.Rows.Count);

                    Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)12.345, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)2147483647, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2016-01-01 19:59:59", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)1, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)2, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("2000-01-03", table.Rows[1]["COL_DATETIME"]);




                    row = this._test3Model.NewRow();
                    row["COL_STR"] = "P001";
                    row["COL_DEC"] = 12.345;
                    row["COL_INT"] = 2147483647;
                    row["COL_DATETIME"] = "2016-01-01 19:59:59";

                    errs = (asyncType == 0)
                        ? this._test3Model.Write(row)
                        : await this._test3Model.WriteAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    row = this._test3Model.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 0;
                    row["COL_INT"] = 0;
                    row["COL_DATETIME"] = "2000-01-01";

                    errs = (asyncType == 0)
                        ? this._test3Model.Write(row)
                        : await this._test3Model.WriteAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                                ? this._test3Model.FindAll("", "COL_STR")
                                : await this._test3Model.FindAllAsync("", "COL_STR");
                    Assert.AreEqual(2, table.Rows.Count);

                    Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)12.345, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)2147483647, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2016-01-01 19:59:59", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("2000-01-01", table.Rows[1]["COL_DATETIME"]);


                    row = this._test3Model.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 1;
                    row["COL_INT"] = 0;
                    row["COL_DATETIME"] = "2000-01-03";

                    errs = (asyncType == 0)
                        ? this._test3Model.Write(row)
                        : await this._test3Model.WriteAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                                ? this._test3Model.FindAll("", "COL_STR")
                                : await this._test3Model.FindAllAsync("", "COL_STR");
                    Assert.AreEqual(2, table.Rows.Count);

                    Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)12.345, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)2147483647, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2016-01-01 19:59:59", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)1, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("2000-01-03", table.Rows[1]["COL_DATETIME"]);

                }
            }

            this.Out("WriteAsyncTest End.");
        }


        [TestMethod()]
        public async Task InsertAsyncTest()
        {
            this.Out("InsertAsyncTest Start.");

            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    this.InitTables(false);
                    this.InitModel(constructorType == 0);

                    var row = this._testModel.NewRow();
                    row["COL_STR"] = "P001";
                    row["COL_DEC"] = 12.345;
                    row["COL_INT"] = 2147483647;
                    row["COL_DATETIME"] = "2016-01-01 19:59:59";

                    var errs = (asyncType == 0)
                        ? this._testModel.Insert(row)
                        : await this._testModel.InsertAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    row = this._testModel.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 0;
                    row["COL_INT"] = 0;
                    row["COL_DATETIME"] = "2000-01-01";

                    errs = await this._testModel.InsertAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    var table = (asyncType == 0)
                        ? this._testModel.FindAll("", "COL_STR")
                        : await this._testModel.FindAllAsync("", "COL_STR");
                    Assert.AreEqual(2, table.Rows.Count);

                    Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)12.345, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)2147483647, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2016-01-01 19:59:59", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("2000-01-01", table.Rows[1]["COL_DATETIME"]);



                    row = this._test3Model.NewRow();
                    row["COL_STR"] = "P001";
                    row["COL_DEC"] = 12.345;
                    row["COL_INT"] = 2147483647;
                    row["COL_DATETIME"] = "2016-01-01 19:59:59";

                    errs = (asyncType == 0)
                        ? this._test3Model.Insert(row)
                        : await this._test3Model.InsertAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    row = this._test3Model.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 0;
                    row["COL_INT"] = 0;
                    row["COL_DATETIME"] = "2000-01-01"; //new DateTime(2000, 1, 1);

                    errs = (asyncType == 0)
                        ? this._test3Model.Insert(row)
                        : await this._test3Model.InsertAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                        ? this._test3Model.FindAll("", "COL_STR")
                        : await this._test3Model.FindAllAsync("", "COL_STR");
                    Assert.AreEqual(2, table.Rows.Count);

                    Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)12.345, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)2147483647, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2016-01-01 19:59:59", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("2000-01-01", table.Rows[1]["COL_DATETIME"]);
                }
            }

            this.Out("InsertAsyncTest End.");
        }


        [TestMethod()]
        public async Task UpdateAsyncTest()
        {
            this.Out("UpdateAsyncTest Start.");

            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    this.InitTables(false);
                    this.InitModel(constructorType == 0);

                    var db = (constructorType == 0)
                        ? this._dbDirect
                        : this._dbRef;

                    var insert = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                                 + ") VALUES ( "
                                 + " '{1}', {2}, {3}, '{4}') ";
                    var sql = string.Format(insert, "test", "P001", 12.345, 1234, "2000-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));
                    sql = string.Format(insert, "test", "P002", 0, 0, "1900-02-03 13:45:12");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));
                    sql = string.Format(insert, "test", "P003", 1, 1, "1901-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));
                    sql = string.Format(insert, "test", "P004", 2, 2, "1902-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    var table = (asyncType == 0)
                        ? this._testModel.FindAll("COL_STR='P002' AND COL_DEC=0")
                        : await this._testModel.FindAllAsync("COL_STR='P002' AND COL_DEC=0");
                    Assert.AreEqual(1, table.Rows.Count);
                    Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("1900-02-03 13:45:12", table.Rows[0]["COL_DATETIME"]);

                    var row = this._testModel.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 0;
                    row["COL_INT"] = 3;
                    row["COL_DATETIME"] = "2010-03-04";

                    var errs = (asyncType == 0)
                        ? this._testModel.Update(row, new string[] { "COL_STR", "COL_DEC" })
                        : await this._testModel.UpdateAsync(row, new string[] { "COL_STR", "COL_DEC" });
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                        ? this._testModel.FindAll("COL_STR='P002' AND COL_DEC=0")
                        : await this._testModel.FindAllAsync("COL_STR='P002' AND COL_DEC=0");

                    Assert.AreEqual(1, table.Rows.Count);
                    Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)3, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2010-03-04", table.Rows[0]["COL_DATETIME"]);



                    sql = string.Format(insert, "test2", "P001", 12.345, 1234, "2000-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test2", "P002", 0, 0, "1900-02-03 13:45:12");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test2", "P003", 1, 1, "1901-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test2", "P004", 2, 2, "1902-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    table = (asyncType == 0)
                        ? this._test2Model.FindAll("COL_STR='P002'")
                        : await this._test2Model.FindAllAsync("COL_STR='P002'");

                    Assert.AreEqual(1, table.Rows.Count);
                    Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("1900-02-03 13:45:12", table.Rows[0]["COL_DATETIME"]);

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 0;
                    row["COL_INT"] = 3;
                    row["COL_DATETIME"] = "2010-03-04";

                    errs = (asyncType == 0)
                        ? this._test2Model.Update(row)
                        : await this._test2Model.UpdateAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                        ? this._test2Model.FindAll("COL_STR='P002' AND COL_DEC=0")
                        : await this._test2Model.FindAllAsync("COL_STR='P002' AND COL_DEC=0");

                    Assert.AreEqual(1, table.Rows.Count);
                    Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)3, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2010-03-04", table.Rows[0]["COL_DATETIME"]);



                    sql = string.Format(insert, "test3", "P001", 12.345, 1234, "2000-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test3", "P002", 0, 0, "1900-02-03 13:45:12");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test3", "P003", 1, 1, "1901-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test3", "P004", 2, 2, "1902-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    table = (asyncType == 0)
                        ? this._test3Model.FindAll("COL_STR='P002'")
                        : await this._test3Model.FindAllAsync("COL_STR='P002'");

                    Assert.AreEqual(1, table.Rows.Count);
                    Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("1900-02-03 13:45:12", table.Rows[0]["COL_DATETIME"]);

                    row = this._test3Model.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 0;
                    row["COL_INT"] = 0;
                    row["COL_DATETIME"] = "2010-03-04";

                    errs = (asyncType == 0)
                        ? this._test3Model.Update(row)
                        : await this._test3Model.UpdateAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                        ? this._test3Model.FindAll("COL_STR='P002' AND COL_DEC=0")
                        : await this._test3Model.FindAllAsync("COL_STR='P002' AND COL_DEC=0");

                    Assert.AreEqual(1, table.Rows.Count);
                    Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2010-03-04", table.Rows[0]["COL_DATETIME"]);
                }
            }

            this.Out("UpdateAsyncTest End.");
        }


        [TestMethod()]
        public async Task DeleteAsyncTest()
        {
            this.Out("DeleteAsyncTest Start.");

            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    this.InitTables(false);
                    this.InitModel(constructorType == 0);

                    var db = (constructorType == 0)
                        ? this._dbDirect
                        : this._dbRef;


                    var insert = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                                 + ") VALUES ( "
                                 + " '{1}', {2}, {3}, '{4}') ";
                    var sql = "";

                    sql = string.Format(insert, "test", "P001", 12.345, 1234, "2000-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test", "P002", 0, 0, "1900-02-03 13:45:12");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test", "P003", 1, 1, "1901-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test", "P004", 2, 2, "1902-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    var table = (asyncType == 0)
                        ? this._testModel.FindAll("COL_STR='P002'")
                        : await this._testModel.FindAllAsync("COL_STR='P002'");

                    Assert.AreEqual(1, table.Rows.Count);
                    Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("1900-02-03 13:45:12", table.Rows[0]["COL_DATETIME"]);

                    var row = this._testModel.NewRow();
                    row["COL_STR"] = "P002";
                    var errs = (asyncType == 0)
                        ? this._testModel.Delete(row, "COL_STR")
                        : await this._testModel.DeleteAsync(row, "COL_STR");
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                        ? this._testModel.FindAll("COL_STR='P002'")
                        : await this._testModel.FindAllAsync("COL_STR='P002'");
                    Assert.AreEqual(0, table.Rows.Count);



                    sql = string.Format(insert, "test2", "P001", 12.345, 1234, "2000-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test2", "P002", 0, 0, "1900-02-03 13:45:12");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test2", "P003", 1, 1, "1901-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test2", "P004", 2, 2, "1902-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    table = (asyncType == 0)
                        ? this._test2Model.FindAll("COL_STR='P002'")
                        : await this._test2Model.FindAllAsync("COL_STR='P002'");

                    Assert.AreEqual(1, table.Rows.Count);
                    Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("1900-02-03 13:45:12", table.Rows[0]["COL_DATETIME"]);

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P002";
                    errs = (asyncType == 0)
                        ? this._test2Model.Delete(row)
                        : await this._test2Model.DeleteAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                        ? this._test2Model.FindAll("COL_STR='P002'")
                        : await this._test2Model.FindAllAsync("COL_STR='P002'");

                    Assert.AreEqual(0, table.Rows.Count);



                    sql = string.Format(insert, "test3", "P001", 12.345, 1234, "2000-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test3", "P002", 0, 0, "1900-02-03 13:45:12");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test3", "P003", 1, 1, "1901-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test3", "P004", 2, 2, "1902-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    table = (asyncType == 0)
                        ? this._test3Model.FindAll("COL_STR='P002'")
                        : await this._test3Model.FindAllAsync("COL_STR='P002'");

                    Assert.AreEqual(1, table.Rows.Count);
                    Assert.AreEqual("P002", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("1900-02-03 13:45:12", table.Rows[0]["COL_DATETIME"]);

                    row = this._test3Model.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_INT"] = 0;
                    errs = (asyncType == 0)
                        ? this._test3Model.Delete(row)
                        : await this._test3Model.DeleteAsync(row);
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                        ? this._test3Model.FindAll("COL_STR='P002'")
                        : await this._test3Model.FindAllAsync("COL_STR='P002'");
                    Assert.AreEqual(0, table.Rows.Count);
                }
            }

            this.Out("DeleteAsyncTest End.");
        }


        [TestMethod()]
        public async Task ReplaceUpdateAsyncTest1()
        {
            this.Out("ReplaceUpdateAsyncTest1 Start.");

            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    this.InitTables(false);
                    this.InitModel(constructorType == 0);

                    var rows = new List<ResultRow>();
                    ResultRow row;

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P001";
                    row["COL_DEC"] = 12.345;
                    row["COL_INT"] = 1234;
                    row["COL_DATETIME"] = "2000-01-01";
                    rows.Add(row);

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 0;
                    row["COL_INT"] = 0;
                    row["COL_DATETIME"] = "1900-02-03 13:45:12";
                    rows.Add(row);

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P003";
                    row["COL_DEC"] = 1;
                    row["COL_INT"] = 1;
                    row["COL_DATETIME"] = "1901-01-01";
                    rows.Add(row);

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P004";
                    row["COL_DEC"] = 2;
                    row["COL_INT"] = 2;
                    row["COL_DATETIME"] = "1902-01-01";
                    rows.Add(row);

                    var errs = (asyncType == 0)
                        ? this._test2Model.ReplaceUpdate(rows)
                        : await this._test2Model.ReplaceUpdateAsync(rows);
                    Assert.AreEqual(0, errs.Length);

                    var table = (asyncType == 0)
                        ? this._test2Model.FindAll("", "COL_STR")
                        : await this._test2Model.FindAllAsync("", "COL_STR");
                    Assert.AreEqual(4, table.Rows.Count);

                    Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)12.345, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)1234, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2000-01-01", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)0, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)0, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("1900-02-03 13:45:12", table.Rows[1]["COL_DATETIME"]);

                    Assert.AreEqual("P003", table.Rows[2]["COL_STR"]);
                    Assert.AreEqual((double)1, table.Rows[2]["COL_DEC"]);
                    Assert.AreEqual((long)1, table.Rows[2]["COL_INT"]);
                    Assert.AreEqual("1901-01-01", table.Rows[2]["COL_DATETIME"]);

                    Assert.AreEqual("P004", table.Rows[3]["COL_STR"]);
                    Assert.AreEqual((double)2, table.Rows[3]["COL_DEC"]);
                    Assert.AreEqual((long)2, table.Rows[3]["COL_INT"]);
                    Assert.AreEqual("1902-01-01", table.Rows[3]["COL_DATETIME"]);


                    var newRows = new List<ResultRow>();
                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P001";
                    row["COL_DEC"] = 1;
                    row["COL_INT"] = 1;
                    row["COL_DATETIME"] = "1901-01-01";
                    newRows.Add(row);

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P003";
                    row["COL_DEC"] = 3;
                    row["COL_INT"] = 3;
                    row["COL_DATETIME"] = "1903-03-03";
                    newRows.Add(row);

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P004";
                    row["COL_DEC"] = 4;
                    row["COL_INT"] = 4;
                    row["COL_DATETIME"] = "1904-04-04";
                    newRows.Add(row);

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P005";
                    row["COL_DEC"] = 5;
                    row["COL_INT"] = 5;
                    row["COL_DATETIME"] = "1905-05-05";
                    newRows.Add(row);

                    row = this._test2Model.NewRow();
                    row["COL_STR"] = "P006";
                    row["COL_DEC"] = 6;
                    row["COL_INT"] = 6;
                    row["COL_DATETIME"] = "1906-06-06";
                    newRows.Add(row);

                    errs = (asyncType == 0)
                        ? this._test2Model.ReplaceUpdate(newRows, rows, "COL_DATETIME")
                        : await this._test2Model.ReplaceUpdateAsync(newRows, rows, "COL_DATETIME");
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                        ? this._test2Model.FindAll("", "COL_STR")
                        : await this._test2Model.FindAllAsync("", "COL_STR");
                    Assert.AreEqual(5, table.Rows.Count);

                    Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)1, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)1, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2000-01-01", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P003", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)3, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)3, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("1901-01-01", table.Rows[1]["COL_DATETIME"]);

                    Assert.AreEqual("P004", table.Rows[2]["COL_STR"]);
                    Assert.AreEqual((double)4, table.Rows[2]["COL_DEC"]);
                    Assert.AreEqual((long)4, table.Rows[2]["COL_INT"]);
                    Assert.AreEqual("1902-01-01", table.Rows[2]["COL_DATETIME"]);

                    Assert.AreEqual("P005", table.Rows[3]["COL_STR"]);
                    Assert.AreEqual((double)5, table.Rows[3]["COL_DEC"]);
                    Assert.AreEqual((long)5, table.Rows[3]["COL_INT"]);
                    Assert.AreEqual("1905-05-05", table.Rows[3]["COL_DATETIME"]);

                    Assert.AreEqual("P006", table.Rows[4]["COL_STR"]);
                    Assert.AreEqual((double)6, table.Rows[4]["COL_DEC"]);
                    Assert.AreEqual((long)6, table.Rows[4]["COL_INT"]);
                    Assert.AreEqual("1906-06-06", table.Rows[4]["COL_DATETIME"]);



                    errs = await this._test2Model.ReplaceUpdateAsync(newRows);
                    Assert.AreEqual(0, errs.Length);

                    table = (asyncType == 0)
                        ? this._test2Model.FindAll("", "COL_STR")
                        : await this._test2Model.FindAllAsync("", "COL_STR");
                    Assert.AreEqual(5, table.Rows.Count);

                    Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)1, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)1, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("1901-01-01", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P003", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)3, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)3, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("1903-03-03", table.Rows[1]["COL_DATETIME"]);

                    Assert.AreEqual("P004", table.Rows[2]["COL_STR"]);
                    Assert.AreEqual((double)4, table.Rows[2]["COL_DEC"]);
                    Assert.AreEqual((long)4, table.Rows[2]["COL_INT"]);
                    Assert.AreEqual("1904-04-04", table.Rows[2]["COL_DATETIME"]);

                    Assert.AreEqual("P005", table.Rows[3]["COL_STR"]);
                    Assert.AreEqual((double)5, table.Rows[3]["COL_DEC"]);
                    Assert.AreEqual((long)5, table.Rows[3]["COL_INT"]);
                    Assert.AreEqual("1905-05-05", table.Rows[3]["COL_DATETIME"]);

                    Assert.AreEqual("P006", table.Rows[4]["COL_STR"]);
                    Assert.AreEqual((double)6, table.Rows[4]["COL_DEC"]);
                    Assert.AreEqual((long)6, table.Rows[4]["COL_INT"]);
                    Assert.AreEqual("1906-06-06", table.Rows[4]["COL_DATETIME"]);
                }
            }
            this.Out("ReplaceUpdateAsyncTest1 End.");
        }


        [TestMethod()]
        public async Task ReplaceUpdateAsyncTest2()
        {
            this.Out("ReplaceUpdateAsyncTest2 Start.");

            for (var asyncType = 0; asyncType < 2; asyncType++)
            {
                for (var constructorType = 0; constructorType < 2; constructorType++)
                {
                    this.InitTables(false);
                    this.InitModel(constructorType == 0);

                    var db = (constructorType == 0)
                        ? this._dbDirect
                        : this._dbRef;

                    var insert = "INSERT INTO {0} (COL_STR, COL_DEC, COL_INT, COL_DATETIME"
                                 + ") VALUES ( "
                                 + " '{1}', {2}, {3}, '{4}') ";
                    var sql = "";

                    sql = string.Format(insert, "test3", "P001", 1, 1, "2001-01-01");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test3", "P002", 2, 2, "2002-02-02");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test3", "P003", 3, 3, "2003-03-03");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    sql = string.Format(insert, "test3", "P004", 4, 4, "2004-04-04");
                    Assert.AreEqual(1, (asyncType == 0)
                        ? db.Execute(sql)
                        : await db.ExecuteAsync(sql));

                    var oldTable = (asyncType == 0)
                        ? this._test3Model.FindAll("", "COL_STR")
                        : await this._test3Model.FindAllAsync("", "COL_STR");

                    Assert.AreEqual(4, oldTable.Rows.Count);

                    Assert.AreEqual("P001", oldTable.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)1, oldTable.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)1, oldTable.Rows[0]["COL_INT"]);
                    Assert.AreEqual("2001-01-01", oldTable.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P002", oldTable.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)2, oldTable.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)2, oldTable.Rows[1]["COL_INT"]);
                    Assert.AreEqual("2002-02-02", oldTable.Rows[1]["COL_DATETIME"]);

                    Assert.AreEqual("P003", oldTable.Rows[2]["COL_STR"]);
                    Assert.AreEqual((double)3, oldTable.Rows[2]["COL_DEC"]);
                    Assert.AreEqual((long)3, oldTable.Rows[2]["COL_INT"]);
                    Assert.AreEqual("2003-03-03", oldTable.Rows[2]["COL_DATETIME"]);

                    Assert.AreEqual("P004", oldTable.Rows[3]["COL_STR"]);
                    Assert.AreEqual((double)4, oldTable.Rows[3]["COL_DEC"]);
                    Assert.AreEqual((long)4, oldTable.Rows[3]["COL_INT"]);
                    Assert.AreEqual("2004-04-04", oldTable.Rows[3]["COL_DATETIME"]);

                    var newTable = (asyncType == 0)
                        ? this._test3Model.FindAll("", "COL_STR")
                        : await this._test3Model.FindAllAsync("", "COL_STR");
                    newTable.Rows.Clear();

                    var row = newTable.NewRow();
                    row["COL_STR"] = "P001";
                    row["COL_DEC"] = 10;
                    row["COL_INT"] = 1;
                    row["COL_DATETIME"] = "1901-02-03";
                    newTable.Rows.Add(row);

                    row = newTable.NewRow();
                    row["COL_STR"] = "P002";
                    row["COL_DEC"] = 20;
                    row["COL_INT"] = 2;
                    row["COL_DATETIME"] = "1902-03-04";
                    newTable.Rows.Add(row);

                    row = newTable.NewRow();
                    row["COL_STR"] = "P004";
                    row["COL_DEC"] = 40;
                    row["COL_INT"] = 4;
                    row["COL_DATETIME"] = "1904-05-06";
                    newTable.Rows.Add(row);

                    row = newTable.NewRow();
                    row["COL_STR"] = "P005";
                    row["COL_DEC"] = 50;
                    row["COL_INT"] = 5;
                    row["COL_DATETIME"] = "1905-06-07";
                    newTable.Rows.Add(row);

                    row = newTable.NewRow();
                    row["COL_STR"] = "P006";
                    row["COL_DEC"] = 60;
                    row["COL_INT"] = 6;
                    row["COL_DATETIME"] = "1906-07-08";
                    newTable.Rows.Add(row);

                    var errs = (asyncType == 0)
                        ? this._test3Model.ReplaceUpdate(newTable, oldTable)
                        : await this._test3Model.ReplaceUpdateAsync(newTable, oldTable);
                    Assert.AreEqual(0, errs.Length);

                    var table = (asyncType == 0)
                        ? this._test3Model.FindAll("", "COL_STR")
                        : await this._test3Model.FindAllAsync("", "COL_STR");
                    Assert.AreEqual(5, table.Rows.Count);

                    Assert.AreEqual("P001", table.Rows[0]["COL_STR"]);
                    Assert.AreEqual((double)10, table.Rows[0]["COL_DEC"]);
                    Assert.AreEqual((long)1, table.Rows[0]["COL_INT"]);
                    Assert.AreEqual("1901-02-03", table.Rows[0]["COL_DATETIME"]);

                    Assert.AreEqual("P002", table.Rows[1]["COL_STR"]);
                    Assert.AreEqual((double)20, table.Rows[1]["COL_DEC"]);
                    Assert.AreEqual((long)2, table.Rows[1]["COL_INT"]);
                    Assert.AreEqual("1902-03-04", table.Rows[1]["COL_DATETIME"]);

                    Assert.AreEqual("P004", table.Rows[2]["COL_STR"]);
                    Assert.AreEqual((double)40, table.Rows[2]["COL_DEC"]);
                    Assert.AreEqual((long)4, table.Rows[2]["COL_INT"]);
                    Assert.AreEqual("1904-05-06", table.Rows[2]["COL_DATETIME"]);

                    Assert.AreEqual("P005", table.Rows[3]["COL_STR"]);
                    Assert.AreEqual((double)50, table.Rows[3]["COL_DEC"]);
                    Assert.AreEqual((long)5, table.Rows[3]["COL_INT"]);
                    Assert.AreEqual("1905-06-07", table.Rows[3]["COL_DATETIME"]);

                    Assert.AreEqual("P006", table.Rows[4]["COL_STR"]);
                    Assert.AreEqual((double)60, table.Rows[4]["COL_DEC"]);
                    Assert.AreEqual((long)6, table.Rows[4]["COL_INT"]);
                    Assert.AreEqual("1906-07-08", table.Rows[4]["COL_DATETIME"]);
                }
            }
            this.Out("ReplaceUpdateAsyncTest2 End.");
        }
    }
}
