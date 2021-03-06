﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestXb.Db;
using Xb.Db;

namespace TestsXb
{
    [TestClass()]
    public class SqliteColumnTests : SqliteBase, IDisposable
    {
        public SqliteColumnTests() : base(true)
        {
        }

        private Xb.Db.Model _testModel;
        private Xb.Db.Model _test2Model;
        private Xb.Db.Model _test3Model;


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
        public void ColumnTest()
        {
            this.Out("ColumnTest Start.");

            for (var constructorType = 0; constructorType < 2; constructorType++)
            {
                this.InitModel(constructorType == 0);

                //以下の文字数/桁数値は、Xb.Db.Sqlite定義の初期値。
                //SQLite仕様上の最大値は別途参照のこと
                //参考：Implementation Limits For SQLite
                //https://www.sqlite.org/limits.html

                var col = this._testModel.GetColumn("COL_STR");
                Assert.AreEqual("COL_STR", col.Name);
                Assert.AreEqual(65335, col.MaxLength);
                Assert.AreEqual(-1, col.MaxInteger);    //文字列なので整数桁値は存在しない -> -1
                Assert.AreEqual(-1, col.MaxDecimal);    //文字列なので少数桁値は存在しない -> -1
                Assert.AreEqual(Xb.Db.Model.Column.ColumnType.String, col.Type);
                Assert.IsFalse(col.IsPrimaryKey);
                Assert.IsFalse(col.IsNullable);

                col = this._test3Model.GetColumn("COL_STR");
                Assert.AreEqual("COL_STR", col.Name);
                Assert.AreEqual(65335, col.MaxLength);
                Assert.AreEqual(-1, col.MaxInteger);    //文字列なので整数桁値は存在しない -> -1
                Assert.AreEqual(-1, col.MaxDecimal);    //文字列なので少数桁値は存在しない -> -1
                Assert.AreEqual(Xb.Db.Model.Column.ColumnType.String, col.Type);
                Assert.IsTrue(col.IsPrimaryKey);
                Assert.IsFalse(col.IsNullable);

                col = this._test3Model.GetColumn("COL_INT"); //2147483647
                Assert.AreEqual("COL_INT", col.Name);
                Assert.AreEqual(13, col.MaxLength); //マイナス符号があるため、12桁＋1
                Assert.AreEqual(12, col.MaxInteger);
                Assert.AreEqual(0, col.MaxDecimal);
                Assert.AreEqual(Xb.Db.Model.Column.ColumnType.Number, col.Type);
                Assert.IsTrue(col.IsPrimaryKey);
                Assert.IsFalse(col.IsNullable);

                col = this._testModel.GetColumn("COL_DEC");
                Assert.AreEqual("COL_DEC", col.Name);
                Assert.AreEqual(14, col.MaxLength); //小数点、マイナス符号があるため、5桁＋2
                Assert.AreEqual(6, col.MaxInteger);
                Assert.AreEqual(6, col.MaxDecimal);
                Assert.AreEqual(Xb.Db.Model.Column.ColumnType.Number, col.Type);
                Assert.IsFalse(col.IsPrimaryKey);
                Assert.IsTrue(col.IsNullable);
            }

            this.Out("ColumnTest End.");
        }

        [TestMethod()]
        public void ValidateTest()
        {
            for (var constructorType = 0; constructorType < 2; constructorType++)
            {
                this.InitModel(constructorType == 0);

                var col = this._testModel.GetColumn("COL_STR");
                col.MaxLength = 10;
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate("a"));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate("あ"));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate("@"));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate("1234567890"));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.LengthOver, col.Validate("１２３４５６７８９０"));

                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NotPermittedNull, col.Validate(null));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.LengthOver, col.Validate("12345678901"));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.LengthOver, col.Validate("１２３４５６７８９０1"));

                col = this._testModel.GetColumn("COL_DEC");
                col.MaxInteger = 2;
                col.MaxDecimal = 3;
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(1));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(12));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(12.3));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(12.34));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(12.345));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(99.999));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(-99.999));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate("99.999"));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate("-99.999"));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.DecimalOver, col.Validate(12.3456));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.DecimalOver, col.Validate(-12.3456));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NotNumber, col.Validate("a"));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(null));

                col = this._test3Model.GetColumn("COL_INT");
                col.MaxInteger = 5;
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(12));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(-12));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.DecimalOver, col.Validate(12.3));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.DecimalOver, col.Validate(-12.3));
                Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NotPermittedNull, col.Validate(null));

                //SQLite hasn't DateTime Type.
                //col = this._test3Model.GetColumn("COL_DATETIME");
                //Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate(new DateTime(2000, 1, 1)));
                //Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate("1900-1-1"));
                //Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate("2000/1/1"));
                //Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NotDateTime, col.Validate("2010/19/19"));
                //Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate("2000/1/1 1:1:1"));
                //Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NoError, col.Validate("2000/1/1 23:59:59"));
                //Assert.AreEqual(Xb.Db.Model.Error.ErrorType.NotDateTime, col.Validate("2000/1/1 24:59:59"));
            }
        }

        [TestMethod()]
        public void GetSqlValueTest()
        {
            Xb.Db.Model.Column col;

            for (var constructorType = 0; constructorType < 2; constructorType++)
            {
                this.InitModel(constructorType == 0);

                col = this._testModel.GetColumn("COL_STR");
                Assert.AreEqual("''", col.GetSqlValue(""));
                Assert.AreEqual("'a'", col.GetSqlValue("a"));
                Assert.AreEqual("'1234567890'", col.GetSqlValue("1234567890"));
                Assert.AreEqual("'123456''890'", col.GetSqlValue("123456'890"));
                Assert.AreEqual("null", col.GetSqlValue(null));

                col = this._testModel.GetColumn("COL_DEC");
                Assert.AreEqual("0", col.GetSqlValue(0));
                Assert.AreEqual("99.999", col.GetSqlValue(99.999));
                Assert.AreEqual("-99.999", col.GetSqlValue(-99.999));
                Assert.AreEqual("null", col.GetSqlValue(null));

                col = this._test3Model.GetColumn("COL_INT");
                Assert.AreEqual("0", col.GetSqlValue(0));
                Assert.AreEqual("123456", col.GetSqlValue(123456));
                Assert.AreEqual("-123456", col.GetSqlValue(-123456));
                Assert.AreEqual("null", col.GetSqlValue(null));

                //SQLite hasn't DateTime Type.
                //col = this._test3Model.GetColumn("COL_DATETIME");
                //Assert.AreEqual("'2000-01-01 00:00:00'", col.GetSqlValue(new DateTime(2000, 1, 1)));
                //Assert.AreEqual("'1900-01-01 00:00:00'", col.GetSqlValue("1900-1-1"));
                //Assert.AreEqual("'2000-01-01 00:00:00'", col.GetSqlValue("2000/1/1"));
                //Assert.AreEqual("'2000-01-01 01:01:01'", col.GetSqlValue("2000/1/1 1:1:1"));
                //Assert.AreEqual("'2000-01-01 23:59:59'", col.GetSqlValue("2000/1/1 23:59:59"));
                //Assert.AreEqual("null", col.GetSqlValue(null));
            }
        }

        [TestMethod()]
        public void GetSqlFormulaTest()
        {
            Xb.Db.Model.Column col;

            for (var constructorType = 0; constructorType < 2; constructorType++)
            {
                this.InitModel(constructorType == 0);

                col = this._testModel.GetColumn("COL_STR");
                Assert.AreEqual("(COL_STR = '')", col.GetSqlFormula(""));
                Assert.AreEqual("(COL_STR = 'a')", col.GetSqlFormula("a"));
                Assert.AreEqual("(COL_STR = '1234567890')", col.GetSqlFormula("1234567890"));
                Assert.AreEqual("(COL_STR = '123456''890')", col.GetSqlFormula("123456'890"));
                Assert.AreEqual("(COL_STR = null)", col.GetSqlFormula(null));

                col = this._testModel.GetColumn("COL_DEC");
                Assert.AreEqual("(COL_DEC = 0)", col.GetSqlFormula(0));
                Assert.AreEqual("(COL_DEC = 99.999)", col.GetSqlFormula(99.999));
                Assert.AreEqual("(COL_DEC = -99.999)", col.GetSqlFormula(-99.999));
                Assert.AreEqual("(COL_DEC = null)", col.GetSqlFormula(null));

                col = this._test3Model.GetColumn("COL_INT");
                Assert.AreEqual("(COL_INT = 0)", col.GetSqlFormula(0));
                Assert.AreEqual("(COL_INT = 123456)", col.GetSqlFormula(123456));
                Assert.AreEqual("(COL_INT = -123456)", col.GetSqlFormula(-123456));
                Assert.AreEqual("(COL_INT = null)", col.GetSqlFormula(null));

                //SQLite hasn't DateTime Type.
                //col = this._test3Model.GetColumn("COL_DATETIME");
                //Assert.AreEqual("(COL_DATETIME = '2000-01-01 00:00:00')", col.GetSqlFormula(new DateTime(2000, 1, 1)));
                //Assert.AreEqual("(COL_DATETIME = '1900-01-01 00:00:00')", col.GetSqlFormula("1900-1-1"));
                //Assert.AreEqual("(COL_DATETIME = '2000-01-01 00:00:00')", col.GetSqlFormula("2000/1/1"));
                //Assert.AreEqual("(COL_DATETIME = '2000-01-01 01:01:01')", col.GetSqlFormula("2000/1/1 1:1:1"));
                //Assert.AreEqual("(COL_DATETIME = '2000-01-01 23:59:59')", col.GetSqlFormula("2000/1/1 23:59:59"));
                //Assert.AreEqual("(COL_DATETIME = null)", col.GetSqlFormula(null));
            }
        }

        public override void Dispose()
        {
            this._testModel.Dispose();
            this._test2Model.Dispose();
            this._test3Model.Dispose();
            this._testModel = null;
            this._test2Model = null;
            this._test3Model = null;

            base.Dispose();
        }
    }
}