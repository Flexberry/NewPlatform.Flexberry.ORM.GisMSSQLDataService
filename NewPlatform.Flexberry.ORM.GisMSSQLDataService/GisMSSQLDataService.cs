using ICSSoft.STORMNET.Business;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Types;
using Microsoft.Spatial;
using System.IO;
using ICSSoft.STORMNET.FunctionalLanguage.SQLWhere;
using ICSSoft.STORMNET.FunctionalLanguage;

namespace NewPlatform.Flexberry.ORM
{
    /// <summary>
    /// Сервис данных для работы с объектами ORM для Gis в Microsoft SQL Server.
    /// </summary>
    public class GisMSSQLDataService: MSSQLDataService
    {

        /// <summary>
        /// Вычитка следующей порции данных
        /// </summary>
        /// <param name="state"></param>
        /// <param name="loadingBufferSize"></param>
        /// <returns></returns>
        public override object[][] ReadNext(ref object state, int loadingBufferSize)
        {
            if (state == null || !state.GetType().IsArray)
                return null;
            IDataReader reader = (IDataReader)((object[])state)[1];
            if (reader.Read())
            {
                var arl = new ArrayList();
                int i = 1;
                int fieldCount = reader.FieldCount;
                WellKnownTextSqlFormatter wktFormatter=null;
                List<int> sqlGeographyColumns=new List<int>();
                for (int c = 0; c < fieldCount; c++)
                {
                    if(reader.GetFieldType(c)==typeof(SqlGeography))
                        sqlGeographyColumns.Add(c);
                }
                if(sqlGeographyColumns.Count>0)
                    wktFormatter = WellKnownTextSqlFormatter.Create();

                while (i <= loadingBufferSize || loadingBufferSize == 0)
                {
                    if (i > 1)
                    {
                        if (!reader.Read())
                            break;
                    }

                    object[] tmp = new object[fieldCount];
                    reader.GetValues(tmp);
                    foreach (var c in sqlGeographyColumns)
                    {
                        if (!(tmp[c] is System.DBNull))
                        {
                            SqlGeography sqlGeo = (SqlGeography)tmp[c];
                            tmp[c] = wktFormatter.Read<Geography>(new StringReader(sqlGeo.ToString()));
                        }
                    }

                    arl.Add(tmp);
                    i++;
                }

                object[][] result = (object[][])arl.ToArray(typeof(object[]));

                if (i < loadingBufferSize || loadingBufferSize == 0)
                {
                    reader.Close();
                    IDbConnection connection = (IDbConnection)((object[])state)[0];
                    connection.Close();
                    state = null;
                }
                return result;
            }
            else
            {
                reader.Close();
                IDbConnection connection = (IDbConnection)((object[])state)[0];
                connection.Close();
                state = null;
                return null;
            }
        }


        /// <summary>
        /// конвертация значений в строки запроса
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public override string ConvertValueToQueryValueString(object value)
        {
            if (value != null && value.GetType().IsSubclassOf(typeof(Geography)))
            {
                Geography geo = value as Geography;
                WellKnownTextSqlFormatter wktFormatter = WellKnownTextSqlFormatter.Create();
                StringWriter wr=new StringWriter();
                wktFormatter.Write(geo, wr);
                return $"geography::Parse('{wr.ToString().Replace("SRID=4326;", "")}')";
            }
            return base.ConvertSimpleValueToQueryValueString(value);
        }

        /// <summary>
        /// Преобразовать значение в SQL строку
        /// </summary>
        /// <param name="function">Функция</param>
        /// <param name="convertValue">делегат для преобразования констант</param>
        /// <param name="convertIdentifier">делегат для преобразования идентификаторов</param>
        /// <returns></returns>
        public override string FunctionToSql(
            SQLWhereLanguageDef sqlLangDef,
            Function value,
            delegateConvertValueToQueryValueString convertValue,
            delegatePutIdentifierToBrackets convertIdentifier)
        {
            return base.FunctionToSql(sqlLangDef, value, convertValue, convertIdentifier);
        }


    }
}
