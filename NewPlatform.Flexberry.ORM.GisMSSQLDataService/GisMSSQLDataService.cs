namespace NewPlatform.Flexberry.ORM
{
    using ICSSoft.STORMNET.Business;
    using ICSSoft.STORMNET.Business.Audit;
    using ICSSoft.STORMNET.Business.LINQProvider.Extensions;
    using ICSSoft.STORMNET.FunctionalLanguage.SQLWhere;
    using ICSSoft.STORMNET.FunctionalLanguage;
    using ICSSoft.STORMNET.Security;
    using ICSSoft.STORMNET.Windows.Forms;

    using Microsoft.Spatial;
    using Microsoft.SqlServer.Types;

    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.IO;

    /// <summary>
    /// Сервис данных для работы с объектами ORM для Gis в Microsoft SQL Server.
    /// </summary>
    public class GisMSSQLDataService: MSSQLDataService
    {
        /// <summary>
        /// Создание сервиса данных для MS SQL без параметров.
        /// </summary>
        public GisMSSQLDataService()
        {
        }

        /// <summary>
        /// Создание сервиса данных для MS SQL с указанием настроек проверки полномочий.
        /// </summary>
        /// <param name="securityManager">Менеджер полномочий.</param>
        public GisMSSQLDataService(ISecurityManager securityManager)
            : base(securityManager)
        {
        }

        /// <summary>
        /// Создание сервиса данных для MS SQL с указанием настроек проверки полномочий.
        /// </summary>
        /// <param name="securityManager">Менеджер полномочий.</param>
        /// <param name="auditService">Сервис аудита.</param>
        public GisMSSQLDataService(ISecurityManager securityManager, IAuditService auditService)
            : base(securityManager, auditService)
        {
        }

        /// <summary>
        /// Осуществляет вычитку следующей порции данных.
        /// </summary>
        /// <param name="state">Состояние вычитки.</param>
        /// <param name="loadingBufferSize">Размер буффера.</param>
        /// <returns>Вычитанная порция данных.</returns>
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
        /// Осуществляет конвертацию заданного значения в строки запроса.
        /// </summary>
        /// <param name="value">Значение для конвертации.</param>
        /// <returns>Строка запроса.</returns>
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
        /// Осуществляет преобразование заданного значения в SQL-строку.
        /// </summary>
        /// <param name="sqlLangDef">Определение языка ограничений.</param>
        /// <param name="value">Ограничивающая функция.</param>
        /// <param name="convertValue">Делегат для преобразования констант.</param>
        /// <param name="convertIdentifier">Делегат для преобразования идентификаторов.</param>
        /// <returns>Результирующая SQL-строка.</returns>
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
