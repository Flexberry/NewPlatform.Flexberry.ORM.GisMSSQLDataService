namespace NewPlatform.Flexberry.ORM
{
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
    using ICSSoft.STORMNET.Business.LINQProvider.Extensions;
    using ICSSoft.STORMNET.FunctionalLanguage.SQLWhere;
    using ICSSoft.STORMNET.FunctionalLanguage;
    using ICSSoft.STORMNET.Windows.Forms;
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
                            tmp[c] = wktFormatter.Read<Geography>(new StringReader($"SRID={sqlGeo.STSrid};{sqlGeo.ToString()}"));
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
                return $"geography::STGeomFromText('{geo.GetWKT()}', {geo.GetSRID()})";
            }
            return base.ConvertValueToQueryValueString(value);
        }

        /// <summary>
        /// Преобразовать значение в SQL строку
        /// </summary>
        /// <param name="sqlLangDef">Определение языка ограничений</param>
        /// <param name="value">Функция</param>
        /// <param name="convertValue">делегат для преобразования констант</param>
        /// <param name="convertIdentifier">делегат для преобразования идентификаторов</param>
        /// <returns></returns>
        public override string FunctionToSql(
            SQLWhereLanguageDef sqlLangDef,
            Function value,
            delegateConvertValueToQueryValueString convertValue,
            delegatePutIdentifierToBrackets convertIdentifier)
        {
            ExternalLangDef langDef = sqlLangDef as ExternalLangDef;
            if (value.FunctionDef.StringedView == "GeoIntersects")
            {
                VariableDef varDef = null;
                Geography geo = null;
                if (value.Parameters[0] is VariableDef && value.Parameters[1] is Geography)
                {
                    varDef = value.Parameters[0] as VariableDef;
                    geo = value.Parameters[1] as Geography;
                }
                else if (value.Parameters[1] is VariableDef && value.Parameters[0] is Geography)
                {
                    varDef = value.Parameters[1] as VariableDef;
                    geo = value.Parameters[0] as Geography;
                }
                if (varDef != null && geo != null)
                {
                    return $"{varDef.StringedView}.STIntersects(geography::STGeomFromText('{geo.GetWKT()}', {geo.GetSRID()}))=1";
                }
                if (value.Parameters[0] is VariableDef && value.Parameters[1] is VariableDef)
                {
                    varDef = value.Parameters[0] as VariableDef;
                    VariableDef varDef2 = value.Parameters[1] as VariableDef;
                    return $"{varDef.StringedView}.STIntersects({varDef2.StringedView})=1";
                }
                geo = value.Parameters[0] as Geography;
                var geo2 = value.Parameters[0] as Geography;
                return $"geography::STGeomFromText('{geo.GetWKT()}', {geo.GetSRID()}).STIntersects(geography::STGeomFromText('{geo2.GetWKT()}', {geo2.GetSRID()}))=1";
            }


            return base.FunctionToSql(sqlLangDef, value, convertValue, convertIdentifier);
        }

    }
}
