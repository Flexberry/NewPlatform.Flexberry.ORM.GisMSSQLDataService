namespace NewPlatform.Flexberry.ORM
{
    using ICSSoft.STORMNET.Business;
    using ICSSoft.STORMNET.Business.Audit;
    using ICSSoft.STORMNET.Business.LINQProvider.Extensions;
    using ICSSoft.STORMNET.FunctionalLanguage.SQLWhere;
    using ICSSoft.STORMNET.FunctionalLanguage;
    using ICSSoft.STORMNET.Security;
    using ICSSoft.STORMNET.Windows.Forms;
    using STORMDO = ICSSoft.STORMNET;
    using Microsoft.Spatial;

    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.IO;
    using System.Text;
    using System.Linq;
    using System;

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
        /// Этот метод переопределён, чтобы подключить правильную подготовку гео-данных в запросе.
        /// </summary>
        /// <param name="customizationStruct">
        /// The customization struct.
        /// </param>
        /// <param name="ForReadValues">
        /// The for read values.
        /// </param>
        /// <param name="StorageStruct">
        /// The storage struct.
        /// </param>
        /// <param name="Optimized">
        /// The optimized.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public override string GenerateSQLSelect(
            LoadingCustomizationStruct customizationStruct,
            // ReSharper disable once InconsistentNaming
            bool ForReadValues,
            // ReSharper disable once InconsistentNaming
            out StorageStructForView[] StorageStruct,
            // ReSharper disable once InconsistentNaming
            bool Optimized)
        {
            var sql = base.GenerateSQLSelect(customizationStruct, ForReadValues, out StorageStruct, Optimized);
            var fromPos = sql.IndexOf("FROM (");
            StringBuilder selectClause = new StringBuilder();
            STORMDO.View dataObjectView = customizationStruct.View;
            System.Type[] dataObjectType = customizationStruct.LoadingTypes;
            int lastPos = 0;
            for (int i = 0; i < dataObjectView.Properties.Length; i++)
            {
                var prop = dataObjectView.Properties[i];
                StorageStructForView.PropStorage propStorage = null;
                foreach (var storage in StorageStruct)
                {
                    propStorage = storage.props.FirstOrDefault(p => p.Name == prop.Name);
                    if (propStorage != null && propStorage.Name == prop.Name)
                        break;
                }
                if (propStorage == null || propStorage.propertyType != typeof(Geography) && propStorage.propertyType != typeof(Geometry))
                    continue;
                var propName = PutIdentifierIntoBrackets(prop.Name);
                var scanText = $"{propName},";
                int pos = sql.IndexOf(scanText, lastPos);
                if (pos == -1)
                {
                    scanText = $"{propName}{Environment.NewLine}";
                    pos = sql.IndexOf(scanText, lastPos);
                }
                if (pos == -1)
                    throw new ArgumentException($"Unexpected property name {propName}. Mismatch customizationStruct.View and SELECT clause.");
                if (pos > lastPos)
                {
                    selectClause.Append(sql.Substring(lastPos, pos - lastPos));
                }

                // The SQL-expression returns EWKT representation of the property value.
                var propExprStrings = new string[]
                    {
                        $"CASE WHEN {propName} IS NULL THEN NULL ELSE",
                        $"CONCAT('SRID=',{propName}.STSrid,';',REPLACE({propName}.ToString(),' (','('))",
                        $"END as {propName}",
                    };
                selectClause.Append(sql
                    .Substring(pos, scanText.Length)
                    .Replace(propName, string.Join(" ", propExprStrings)));
                lastPos = pos + scanText.Length;
            }
            if (lastPos < fromPos)
            {
                selectClause.Append(sql.Substring(lastPos, fromPos - lastPos));
            }
            sql = $"{selectClause.ToString()}{sql.Substring(fromPos)}";
            return sql;
        }

        /// <summary>
        /// Осуществляет конвертацию заданного значения в строку запроса.
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

            if (value != null && value.GetType().IsSubclassOf(typeof(Geometry)))
            {
                Geometry geo = value as Geometry;
                return $"geometry::STGeomFromText('{geo.GetWKT()}', {geo.GetSRID()})";
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
            const string GeoDistance = "GeoDistance";
            const string GeomDistance = "GeomDistance";
            const string GeoIntersects = "GeoIntersects";
            const string GeomIntersects = "GeomIntersects";
            const string SqlDistanceFunction = "STDistance";
            const string SqlIntersectsFunction = "STIntersects";

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            if (convertValue == null)
            {
                throw new ArgumentNullException(nameof(convertValue));
            }

            ExternalLangDef langDef = sqlLangDef as ExternalLangDef;
            var sqlFunction = string.Empty;
            var sqlCondition = string.Empty;

            if (value.FunctionDef.StringedView == GeoDistance || value.FunctionDef.StringedView == GeomDistance)
            {
                sqlFunction = SqlDistanceFunction;
            }
            else if (value.FunctionDef.StringedView == GeoIntersects || value.FunctionDef.StringedView == GeomIntersects)
            {
                sqlFunction = SqlIntersectsFunction;
                sqlCondition = "=1";
            }

            if (value.FunctionDef.StringedView == GeoDistance || value.FunctionDef.StringedView == GeoIntersects)
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
                    string sqlIdent = PutIdentifierIntoBrackets(varDef.StringedView);
                    return $"{sqlIdent}.{sqlFunction}({convertValue(geo)}){sqlCondition}";
                }

                if (value.Parameters[0] is VariableDef && value.Parameters[1] is VariableDef)
                {
                    varDef = value.Parameters[0] as VariableDef;
                    VariableDef varDef2 = value.Parameters[1] as VariableDef;
                    string sqlIdent = PutIdentifierIntoBrackets(varDef.StringedView);
                    string sqlIdent2 = PutIdentifierIntoBrackets(varDef2.StringedView);
                    return $"{sqlIdent}.{sqlFunction}({sqlIdent2}){sqlCondition}";
                }

                geo = value.Parameters[0] as Geography;
                var geo2 = value.Parameters[1] as Geography;
                return $"{convertValue(geo)}.{sqlFunction}({convertValue(geo2)}){sqlCondition}";
            }

            if (value.FunctionDef.StringedView == GeomDistance || value.FunctionDef.StringedView == GeomIntersects)
            {
                VariableDef varDef = null;
                Geometry geo = null;

                if (value.Parameters[0] is VariableDef && value.Parameters[1] is Geometry)
                {
                    varDef = value.Parameters[0] as VariableDef;
                    geo = value.Parameters[1] as Geometry;
                }
                else if (value.Parameters[1] is VariableDef && value.Parameters[0] is Geometry)
                {
                    varDef = value.Parameters[1] as VariableDef;
                    geo = value.Parameters[0] as Geometry;
                }

                if (varDef != null && geo != null)
                {
                    string sqlIdent = PutIdentifierIntoBrackets(varDef.StringedView);
                    return $"{sqlIdent}.{sqlFunction}({convertValue(geo)}){sqlCondition}";
                }

                if (value.Parameters[0] is VariableDef && value.Parameters[1] is VariableDef)
                {
                    varDef = value.Parameters[0] as VariableDef;
                    VariableDef varDef2 = value.Parameters[1] as VariableDef;
                    string sqlIdent = PutIdentifierIntoBrackets(varDef.StringedView);
                    string sqlIdent2 = PutIdentifierIntoBrackets(varDef2.StringedView);
                    return $"{sqlIdent}.{sqlFunction}({sqlIdent2}){sqlCondition}";
                }

                geo = value.Parameters[0] as Geometry;
                var geo2 = value.Parameters[1] as Geometry;
                return $"{convertValue(geo)}.{sqlFunction}({convertValue(geo2)}){sqlCondition}";
            }

            return base.FunctionToSql(sqlLangDef, value, convertValue, convertIdentifier);
        }
    }
}
