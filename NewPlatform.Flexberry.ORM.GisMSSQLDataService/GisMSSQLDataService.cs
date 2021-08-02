namespace NewPlatform.Flexberry.ORM
{
    using System;
    using System.Linq;
    using System.Text;

    using ICSSoft.STORMNET.Business;
    using ICSSoft.STORMNET.Business.Audit;
    using ICSSoft.STORMNET.Business.LINQProvider.Extensions;
    using ICSSoft.STORMNET.FunctionalLanguage;
    using ICSSoft.STORMNET.FunctionalLanguage.SQLWhere;
    using ICSSoft.STORMNET.Security;
    using ICSSoft.STORMNET.Windows.Forms;

    using Microsoft.Spatial;

    using STORMDO = ICSSoft.STORMNET;

    /// <summary>
    /// Сервис данных для работы с объектами ORM для Gis в Microsoft SQL Server.
    /// </summary>
    public class GisMSSQLDataService : MSSQLDataService
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
            if (value is Geography geo)
            {
                return $"geography::STGeomFromText('{geo.GetWKT()}', {geo.GetSRID()})";
            }

            if (value is Geometry geom)
            {
                return $"geometry::STGeomFromText('{geom.GetWKT()}', {geom.GetSRID()})";
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
            const string SqlDistanceFunction = "STDistance";
            const string SqlIntersectsFunction = "STIntersects";

            if (sqlLangDef == null)
            {
                throw new ArgumentNullException(nameof(sqlLangDef));
            }

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
            if (value.FunctionDef.StringedView == langDef.funcGeoDistance || value.FunctionDef.StringedView == langDef.funcGeomDistance)
            {
                sqlFunction = SqlDistanceFunction;
            }
            else if (value.FunctionDef.StringedView == langDef.funcGeoIntersects || value.FunctionDef.StringedView == langDef.funcGeomIntersects)
            {
                sqlFunction = SqlIntersectsFunction;
                sqlCondition = "=1";
            }

            if (!string.IsNullOrEmpty(sqlFunction))
            {
                var sqlParameters = new string[2];
                sqlParameters[0] = value.Parameters[0] is VariableDef ?
                    $"{PutIdentifierIntoBrackets((value.Parameters[0] as VariableDef).StringedView)}" : convertValue(value.Parameters[0]);
                sqlParameters[1] = value.Parameters[1] is VariableDef ?
                    $"{PutIdentifierIntoBrackets((value.Parameters[1] as VariableDef).StringedView)}" : convertValue(value.Parameters[1]);

                return $"{sqlParameters[0]}.{sqlFunction}({sqlParameters[1]}){sqlCondition}";
            }

            return base.FunctionToSql(sqlLangDef, value, convertValue, convertIdentifier);
        }
    }
}
