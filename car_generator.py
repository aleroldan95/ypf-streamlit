import json
import pyodbc
import os
import datetime
import pandas as pd
import sqlalchemy
import numpy as np
import datetime as dt
import time
from tqdm import tqdm


class CAR:
    def __init__(self):
        self.execution_path = os.getcwd()
        self.app_conn = None
        self.azure_conn = None
        self.comarch_conn = None

    def generate_sql_connections(self):
        # servers = ["config_APP", "config_azure_SQLCU" , 'config_Comarch_diego']
        self.app_conn = self.server_connection(self.execution_path, 'config_APP')
        self.azure_conn = self.server_connection(self.execution_path, 'config_azure_SQLCU')
        self.comarch_conn = self.server_connection(self.execution_path, 'config_Comarch_diego')

    def close_sql_connections(self):
        self.app_conn.close()
        self.azure_conn.close()
        self.comarch_conn.close()

    def get_sql_connections(self):
        if self.app_conn != None:
            return self.app_conn, self.azure_conn, self.comarch_conn
        else:
            KeyError("No se encuentran generadas las conecciones a los servidores")

    def server_connection(self, execution_path, config_name):
        settings = json.load(open(f'{execution_path}\\credenciales\\{config_name}.json'))
        # Error fixed
        settings["DRIVER"] = "SQL Server"
        conn = pyodbc.connect(
            f'DRIVER={settings["DRIVER"]};SERVER={settings["SERVER"]};DATABASE={settings["DATABASE"]};UID={settings["UID"]};PWD={settings["PWD"]}')
        return conn

    def df_from_query(self, query, con):
        return pd.io.sql.read_sql(query, con=con)

    def app_query(self):
        query = """
        select
            email,
            sum(case when DATEDIFF(dd ,"Date", sysdatetime()) between 0 and 30 
                    then A_Pagar end) as monto_transaccion_app_30d,
            sum(case when DATEDIFF(dd ,"Date", sysdatetime()) between 0 and 60 
                    then A_Pagar end) as monto_transaccion_app_60d,
            sum(case when DATEDIFF(dd ,"Date", sysdatetime()) between 0 and 90 
                    then A_Pagar end) as monto_transaccion_app_90d,
            sum(case when DATEDIFF(dd ,"Date", sysdatetime()) between 0 and 30 
                    then cast(litros as float) end) as litros_transaccion_app_30d,
            sum(case when DATEDIFF(dd ,"Date", sysdatetime()) between 0 and 60 
                    then cast(litros as float) end) as litros_transaccion_app_60d,
            sum(case when DATEDIFF(dd ,"Date", sysdatetime()) between 0 and 90 
                    then cast(litros as float) end) as litros_transaccion_app_90d,
            count(case when DATEDIFF(dd ,"Date", sysdatetime()) between 0 and 30 
                    then email end) as frecuencia_transaccion_app_30d,
            count(case when DATEDIFF(dd ,"Date", sysdatetime()) between 0 and 60 
                    then email end) as frecuencia_transaccion_app_60d,
            count(case when DATEDIFF(dd ,"Date", sysdatetime()) between 0 and 90 
                    then email end) as frecuencia_transaccion_app_90d,
            case when sum(case when Medio_de_pago='CREDIT_CARD' then 1 else 0 end) > 0 then 1 else 0 end as app_bancarizado
            from VW_NFP
            group by email;
    	"""  # 40 seg
        return pd.io.sql.read_sql(query, con=self.app_conn)

    def commarch_query(self):
        query = """
        with aux as(
            select 
                TPD_CUSGID,  --cliente
                TPD_PRDGID, --Tipo de Nafta/Producto
                TPD_PMTGID, --PaymentMethod
                TPD_M_ValueOfProducts, --Valor de la trasaccion
                DATEDIFF(dd ,fecha, sysdatetime()) as diferencia_dias		
            from (
                select 
                    TPD_CUSGID,  --cliente
                    TPD_PRDGID, --Tipo de Nafta/Producto
                    TPD_PMTGID, --PaymentMethod
                    TPD_M_ValueOfProducts, --Valor de la trasaccion
                    datefromparts(left(TPD_TDTTranDateGID,4), right(left(TPD_TDTTranDateGID,6),2), right(TPD_TDTTranDateGID,2)) as fecha
                from (
                    select 
                        TPD_CUSGID,  --cliente
                        TPD_PRDGID, --Tipo de Nafta/Producto
                        TPD_PMTGID, --PaymentMethod
                        TPD_M_ValueOfProducts, --Valor de la trasaccion
                        TPD_TDTTranDateGID --Fecha
                    from FCT_TrnSrcProducts 
                    where 
                        TPD_TDTTranDateGID <>-1
                     ) as p
                 ) as c
            where 
                DATEDIFF(dd ,fecha, sysdatetime()) between 0 and 365
            )
        select
            dim.CUS_GID as id_cliente,
            dim.CUS_Gender as genero,
            dim.CUS_BirthDate as fecha_nacimiento,
            dim.CUSC_DNI as DNI,
            dim.CUSC_IsActive90Days as estado,
            dim.CUS_IsContactable as permite_contacto,
            dim.CUS_PermissionEmail as permite_email,
            dim.CUS_PermissionPhone as permite_telefono,
            dim.CUS_PermissionSMS as permite_sms,
            dim.CUS_Language as idioma,
            dim.CUSC_IsEmployee as es_empleado_ypf,
            sp.saldo_puntos,
            est.estacion_favorita1,
            est.estacion_favorita2,
            est.estacion_favorita3,
            est.antiguedad_serviclub,
            cont.provincia,
            cont.email,
            cont.celular,
            cont.telefono_fijo,
            cont.codigo_postal,
            redim.redim_30,
            redim.redim_60,
            redim.redim_90,
            redim.redim_365,
            rec.recencia_dias,
            frec.frecuencia_365d,
            monto_boxes_30d,
            trs.monto_boxes_60d,
            trs.monto_boxes_90d,
            trs.avg_boxes_365d,
            trs.monto_infinia_30d,
            trs.monto_infinia_60d,
            trs.monto_infinia_90d,
            trs.avg_infinia_365d,
            trs.monto_infinia_diesel_30d,
            trs.monto_infinia_diesel_60d,
            trs.monto_infinia_diesel_90d,
            trs.avg_infinia_diesel_365d,
            trs.monto_super_30d,
            trs.monto_super_60d,
            trs.monto_super_90d,
            trs.avg_super_365d,
            trs.monto_ultra_30d,
            trs.monto_ultra_60d,
            trs.monto_ultra_90d,
            trs.avg_ultra_365d,
            trs.monto_gnc_30d,
            trs.monto_gnc_60d,
            trs.monto_gnc_90d,
            trs.avg_gnc_365d,
            trs.monto_tiendas_30d,
            trs.monto_tiendas_60d,
            trs.monto_tiendas_90d,
            trs.avg_tiendas_365d,
            trs.transaccion_app30d,
            trs.transaccion_app60d,
            trs.transaccion_app90d,
            trs.transaccion_qr30d,
            trs.transaccion_qr60d,
            trs.transaccion_qr90d,
            trs.transaccion_otros30d,
            trs.transaccion_otros60d,
            trs.transaccion_otros90d,
            trs.socio_infinia_30d,
            trs.socio_infinia_60d,
            trs.socio_infinia_90d,
            trs.socio_infinia_diesel_30d,
            trs.socio_infinia_diesel_60d,
            trs.socio_infinia_diesel_90d,
            trs.socio_super_30d,
            trs.socio_super_60d,
            trs.socio_super_90d,
            trs.socio_ultra_30d,
            trs.socio_ultra_60d,
            trs.socio_ultra_90d,
            trs.socio_gnc_30d,
            trs.socio_gnc_60d,
            trs.socio_gnc_90d,
            dim.CUS_LOGIN as email_login
        from (select * from DIM_Customer where CUSC_IsActive90Days = 1) dim
        left join 
            (select 
                ACC_CUSGID,
                sum(ACC_PointsBalance) as saldo_puntos
            from DIM_Account
            group by ACC_CUSGID) as sp
            on sp.ACC_CUSGID=dim.CUS_GID  --Saldo Puntos Done! 3min 41 seg
        left join
            (select 
                ACC_CUSGID,
                ACCC_TheMostFavouriteSite as estacion_favorita1,
                ACCC_TheSecondFavouriteSite as estacion_favorita2,
                ACCC_TheThirdFavouriteSite as estacion_favorita3,
                ACC_EnrolmentDate as antiguedad_serviclub
                from (select 
                        c.*,
                        row_number() over (partition by c.ACC_CUSGID order by ACCC_TheMostFavouriteSite desc) as rn
                    from DIM_Account c) c
                where c.rn=1) as est
            on est.ACC_CUSGID=dim.CUS_GID --5min
        left join
            (select 
                CCD_GID,
                CCD_Region as provincia,
                CCD_Email as email,
                CCD_MobilePhoneNumber as celular,
                CCD_HomePhoneNumber as telefono_fijo,
                CCD_PostalCode as codigo_postal
            from DIM_ContactDetails) as cont
            on cont.CCD_GID=dim.CUS_GID
        left join
            (select 
                RED_ACCGID,
                sum(case when DATEDIFF(dd ,fecha, sysdatetime()) between 0 and 30 
                        then RED_M_RedeemedPoints end) as redim_30,
                sum(case when DATEDIFF(dd ,fecha, sysdatetime()) between 0 and 60 
                        then RED_M_RedeemedPoints end) as redim_60,
                sum(case when DATEDIFF(dd ,fecha, sysdatetime()) between 0 and 90 
                        then RED_M_RedeemedPoints end) as redim_90,
                sum(case when DATEDIFF(dd ,fecha, sysdatetime()) between 0 and 365 
                        then RED_M_RedeemedPoints end) as redim_365
            from (
                    select 
                    RED_ACCGID,
                    RED_M_RedeemedPoints,
                    datefromparts(left(RED_TDTTranDateGID,4), right(left(RED_TDTTranDateGID,6),2), right(RED_TDTTranDateGID,2)) as fecha
                from FCT_RewardsRedemptions) as aux
            group by RED_ACCGID) as redim
            on redim.RED_ACCGID=dim.CUS_GID --1 min 30 seg
        left join
            (select 
                TBS_CUSGID,
                DATEDIFF(dd ,ultima_compra, sysdatetime()) as recencia_dias
            from (
                select 
                    TBS_CUSGID,
                    max(CAST(TBS_TransactionDate AS datetime)) as ultima_compra
                from FCT_TransactionBillings
                group by TBS_CUSGID) as aux) as rec
            on rec.TBS_CUSGID=dim.CUS_GID --3 min 30 seg
        left join
            (select 
                TBS_CUSGID,
                count(*) as frecuencia_365d
            from FCT_TransactionBillings
            where DATEDIFF(dd ,CAST(TBS_TransactionDate AS datetime), sysdatetime()) between 0 and 365
            group by TBS_CUSGID) as frec
            on frec.TBS_CUSGID=dim.CUS_GID --3 min
        left join
            (select 
            *,
            case when monto_infinia_30d > 0 then 1 else 0 end as socio_infinia_30d,
            case when monto_infinia_60d > 0 then 1 else 0 end as socio_infinia_60d,
            case when monto_infinia_90d > 0 then 1 else 0 end as socio_infinia_90d,
            case when monto_infinia_diesel_30d > 0 then 1 else 0 end as socio_infinia_diesel_30d,
            case when monto_infinia_diesel_60d > 0 then 1 else 0 end as socio_infinia_diesel_60d,
            case when monto_infinia_diesel_90d > 0 then 1 else 0 end as socio_infinia_diesel_90d,
            case when monto_super_30d > 0 then 1 else 0 end as socio_super_30d,
            case when monto_super_60d > 0 then 1 else 0 end as socio_super_60d,
            case when monto_super_90d > 0 then 1 else 0 end as socio_super_90d,
            case when monto_ultra_30d > 0 then 1 else 0 end as socio_ultra_30d,
            case when monto_ultra_60d > 0 then 1 else 0 end as socio_ultra_60d,
            case when monto_ultra_90d > 0 then 1 else 0 end as socio_ultra_90d,
            case when monto_gnc_30d > 0 then 1 else 0 end as socio_gnc_30d,
            case when monto_gnc_60d > 0 then 1 else 0 end as socio_gnc_60d,
            case when monto_gnc_90d > 0 then 1 else 0 end as socio_gnc_90d
            from (
                select 
                    dim.CUS_GID as id,
                    sum(case when a.diferencia_dias between 0 and 30 and a.TPD_PRDGID = '100000099' then a.TPD_M_ValueOfProducts end) as monto_boxes_30d,
                    sum(case when a.diferencia_dias between 0 and 60 and a.TPD_PRDGID = '100000099' then a.TPD_M_ValueOfProducts end) as monto_boxes_60d,
                    sum(case when a.diferencia_dias between 0 and 90 and a.TPD_PRDGID = '100000099' then a.TPD_M_ValueOfProducts end) as monto_boxes_90d,
                    avg(case when a.diferencia_dias between 0 and 365 and a.TPD_PRDGID = '100000099' then a.TPD_M_ValueOfProducts end) as avg_boxes_365d,

                    sum(case when a.diferencia_dias between 0 and 30 and a.TPD_PRDGID = '100000095' then a.TPD_M_ValueOfProducts end) as monto_infinia_30d,
                    sum(case when a.diferencia_dias between 0 and 60 and a.TPD_PRDGID = '100000095' then a.TPD_M_ValueOfProducts end) as monto_infinia_60d,
                    sum(case when a.diferencia_dias between 0 and 90 and a.TPD_PRDGID = '100000095' then a.TPD_M_ValueOfProducts end) as monto_infinia_90d,
                    avg(case when a.diferencia_dias between 0 and 365 and a.TPD_PRDGID = '100000095' then a.TPD_M_ValueOfProducts end) as avg_infinia_365d,

                    sum(case when a.diferencia_dias between 0 and 30 and a.TPD_PRDGID = '100000097' then a.TPD_M_ValueOfProducts end) as monto_infinia_diesel_30d,
                    sum(case when a.diferencia_dias between 0 and 60 and a.TPD_PRDGID = '100000097' then a.TPD_M_ValueOfProducts end) as monto_infinia_diesel_60d,
                    sum(case when a.diferencia_dias between 0 and 90 and a.TPD_PRDGID = '100000097' then a.TPD_M_ValueOfProducts end) as monto_infinia_diesel_90d,
                    avg(case when a.diferencia_dias between 0 and 365 and a.TPD_PRDGID = '100000097' then a.TPD_M_ValueOfProducts end) as avg_infinia_diesel_365d,

                    sum(case when a.diferencia_dias between 0 and 30 and a.TPD_PRDGID = '100000093' then a.TPD_M_ValueOfProducts end) as monto_super_30d,
                    sum(case when a.diferencia_dias between 0 and 60 and a.TPD_PRDGID = '100000093' then a.TPD_M_ValueOfProducts end) as monto_super_60d,
                    sum(case when a.diferencia_dias between 0 and 90 and a.TPD_PRDGID = '100000093' then a.TPD_M_ValueOfProducts end) as monto_super_90d,
                    avg(case when a.diferencia_dias between 0 and 365 and a.TPD_PRDGID = '100000093' then a.TPD_M_ValueOfProducts end) as avg_super_365d,

                    sum(case when a.diferencia_dias between 0 and 30 and a.TPD_PRDGID = '100000092' then a.TPD_M_ValueOfProducts end) as monto_ultra_30d,
                    sum(case when a.diferencia_dias between 0 and 60 and a.TPD_PRDGID = '100000092' then a.TPD_M_ValueOfProducts end) as monto_ultra_60d,
                    sum(case when a.diferencia_dias between 0 and 90 and a.TPD_PRDGID = '100000092' then a.TPD_M_ValueOfProducts end) as monto_ultra_90d,
                    avg(case when a.diferencia_dias between 0 and 365 and a.TPD_PRDGID = '100000092' then a.TPD_M_ValueOfProducts end) as avg_ultra_365d,

                    sum(case when a.diferencia_dias between 0 and 30 and a.TPD_PRDGID = '1000000133' then a.TPD_M_ValueOfProducts end) as monto_gnc_30d,
                    sum(case when a.diferencia_dias between 0 and 60 and a.TPD_PRDGID = '1000000133' then a.TPD_M_ValueOfProducts end) as monto_gnc_60d,
                    sum(case when a.diferencia_dias between 0 and 90 and a.TPD_PRDGID = '1000000133' then a.TPD_M_ValueOfProducts end) as monto_gnc_90d,
                    avg(case when a.diferencia_dias between 0 and 365 and a.TPD_PRDGID = '1000000133' then a.TPD_M_ValueOfProducts end) as avg_gnc_365d,

                    sum(case when a.diferencia_dias between 0 and 30 and a.TPD_PRDGID = '100007571' then a.TPD_M_ValueOfProducts end) as monto_tiendas_30d,
                    sum(case when a.diferencia_dias between 0 and 60 and a.TPD_PRDGID = '100007571' then a.TPD_M_ValueOfProducts end) as monto_tiendas_60d,
                    sum(case when a.diferencia_dias between 0 and 90 and a.TPD_PRDGID = '100007571' then a.TPD_M_ValueOfProducts end) as monto_tiendas_90d,
                    avg(case when a.diferencia_dias between 0 and 365 and a.TPD_PRDGID = '100007571' then a.TPD_M_ValueOfProducts end) as avg_tiendas_365d,

        			sum(case when a.diferencia_dias between 0 and 30 and a.TPD_PMTGID = '100000014' then a.TPD_M_ValueOfProducts end) as transaccion_app30d,
                    sum(case when a.diferencia_dias between 0 and 60 and a.TPD_PMTGID = '100000014' then a.TPD_M_ValueOfProducts end) as transaccion_app60d,
                    sum(case when a.diferencia_dias between 0 and 90 and a.TPD_PMTGID = '100000014' then a.TPD_M_ValueOfProducts end) as transaccion_app90d,

                    sum(case when a.diferencia_dias between 0 and 30 and a.TPD_PMTGID = '100000010' then a.TPD_M_ValueOfProducts end) as transaccion_qr30d,
                    sum(case when a.diferencia_dias between 0 and 60 and a.TPD_PMTGID = '100000010' then a.TPD_M_ValueOfProducts end) as transaccion_qr60d,
                    sum(case when a.diferencia_dias between 0 and 90 and a.TPD_PMTGID = '100000010' then a.TPD_M_ValueOfProducts end) as transaccion_qr90d,

                    sum(case when a.diferencia_dias between 0 and 30 and a.TPD_PMTGID NOT IN ('100000010', '100000014') then a.TPD_M_ValueOfProducts end) as transaccion_otros30d,
                    sum(case when a.diferencia_dias between 0 and 60 and a.TPD_PMTGID NOT IN ('100000010', '100000014') then a.TPD_M_ValueOfProducts end) as transaccion_otros60d,
                    sum(case when a.diferencia_dias between 0 and 90 and a.TPD_PMTGID NOT IN ('100000010', '100000014') then a.TPD_M_ValueOfProducts end) as transaccion_otros90d
                    from DIM_Customer dim
                left join aux a on dim.CUS_GID=a.TPD_CUSGID
                group by dim.CUS_GID) as p) as trs
                on trs.id = dim.CUS_GID
        """
        return query

    def merge_commarch_app(self, df_commarch, df_app):
        # df_commarch['registrado_en_app'] = df_commarch['email_login'].isin(list(df_app['email']))
        df_merge = pd.merge(left=df_commarch,
                            right=df_app,
                            how='left',
                            left_on='email_login',
                            right_on='email',
                            suffixes=('_commarch', '_app'),
                            indicator='registrado_en_app')
        if len(df_merge) != len(df_commarch):
            message = "Error al unir commarch con app. Se encontraron duplicidad de valores"
        else:
            message = 'Unión completada con éxito'
        return df_merge, message

    def get_reporting_status(self, df_final, key_columns=['email']):
        """
        Function that evaluates if the filtered dataframe can be used as a final output or not
        :param df_final: dataframe with columns from both queries
        :param key_columns: list of columns that cannot have NAs. If they have NA then all future actions will be halted
        :return: report_status
        """
        # First I check if there are no NAs whatsoever
        if df_final.isnull().values.any():
            # If there are NAs, I evaluate where there are NAs: in any of the key columns or not
            if df_final[key_columns].isnull().values.any():
                # If there are any NAs in any of the key columns, we report the percentage of NAs
                report_status = 'Error'
                report_message = 'No se puede hacer ningún análisis, hay valores nulos en columnas clave.\n'
                report_message = report_message + 'A continuación, se presentan los porcentajes de NAs \n'
                for column in key_columns:
                    perc_nan = round((df_final[column].isnull().sum() / len(df_final)) * 100, 2)
                    if perc_nan > 0:
                        report_message = report_message + f"{column}: {perc_nan}% \n"
            else:
                # There are NAs but not in any of the key columns, we evaluate in which of the columns
                # there are NAs
                report_status = 'Warning'
                report_message = "Aviso. Hay nulos en la base de datos seleccionada \n"
                report_message = report_message + 'A continuación, se presentan los porcentajes de NAs \n'
                for column in list(df_final.columns):
                    perc_nan = round((df_final[column].isnull().sum() / len(df_final)) * 100, 2)
                    if perc_nan > 0:
                        report_message = report_message + f"{column}: {perc_nan}% \n"
        else:
            report_status = 'OK'
            report_message = 'Ninguna columna tiene valores nulos. \n Continuar con el análisis'
        return report_status, report_message

    def insert_new_data(self, df, table_name, columns_types, conn, mode: int = 1):

        aux = []
        for row in df.values:
            aux.append(row)

        for i in range(len(aux)):
            row = aux[i]
            aux[i] = '('
            for x, word in enumerate(row):
                if x == len(row) - 1:
                    if word == None:
                        aux[i] += 'null)'
                    elif columns_types[x] in ['String', 'Date']:
                        aux[i] += '\'' + str(word) + '\')'
                    else:
                        aux[i] += str(word) + ')'
                else:
                    if word == None:
                        aux[i] += 'null, '
                    elif columns_types[x] in ['String', 'Date']:
                        aux[i] += '\'' + str(word) + '\','
                    else:
                        aux[i] += str(word) + ','

        final = ''
        for i in aux:
            final += i + ','
        final = final[:-1]

        columns_names = self.get_car_columns_names()
        columns = ''
        for col in columns_names:
            columns += col + ', '

        if mode == 1:
            query = f"""INSERT INTO {table_name}({columns[:-2]}) SELECT * FROM (VALUES {final}) A({columns[:-2]}) """
        elif mode == 2:
            query = f"""INSERT INTO {table_name} VALUES {final}"""
        elif mode == 3:
            df.to_sql(name=table_name, con=conn, if_exists='replace', schema='azpussql01.database.windows.net')
        conn.execute(query)

    def get_car_columns_names(self):
        return ['id_cliente',
                'genero',
                'fecha_nacimiento',
                'DNI',
                'estado',
                'permite_contacto',
                'permite_email',
                'permite_telefono',
                'permite_sms',
                'idioma',
                'es_empleado_ypf',
                'saldo_puntos',
                'estacion_favorita1',
                'estacion_favorita2',
                'estacion_favorita3',
                'antiguedad_serviclub',
                'provincia',
                'email_commarch',
                'celular',
                'telefono_fijo',
                'codigo_postal',
                'redim_30',
                'redim_60',
                'redim_90',
                'redim_365',
                'recencia_dias',
                'frecuencia_365d',
                'monto_boxes_30d',
                'monto_boxes_60d',
                'monto_boxes_90d',
                'avg_boxes_365d',
                'monto_infinia_30d',
                'monto_infinia_60d',
                'monto_infinia_90d',
                'avg_infinia_365d',
                'monto_infinia_diesel_30d',
                'monto_infinia_diesel_60d',
                'monto_infinia_diesel_90d',
                'avg_infinia_diesel_365d',
                'monto_super_30d',
                'monto_super_60d',
                'monto_super_90d',
                'avg_super_365d',
                'monto_ultra_30d',
                'monto_ultra_60d',
                'monto_ultra_90d',
                'avg_ultra_365d',
                'monto_gnc_30d',
                'monto_gnc_60d',
                'monto_gnc_90d',
                'avg_gnc_365d',
                'monto_tiendas_30d',
                'monto_tiendas_60d',
                'monto_tiendas_90d',
                'avg_tiendas_365d',
                'transaccion_app30d',
                'transaccion_app60d',
                'transaccion_app90d',
                'transaccion_qr30d',
                'transaccion_qr60d',
                'transaccion_qr90d',
                'transaccion_otros30d',
                'transaccion_otros60d',
                'transaccion_otros90d',
                'socio_infinia_30d',
                'socio_infinia_60d',
                'socio_infinia_90d',
                'socio_infinia_diesel_30d',
                'socio_infinia_diesel_60d',
                'socio_infinia_diesel_90d',
                'socio_super_30d',
                'socio_super_60d',
                'socio_super_90d',
                'socio_ultra_30d',
                'socio_ultra_60d',
                'socio_ultra_90d',
                'socio_gnc_30d',
                'socio_gnc_60d',
                'socio_gnc_90d',
                'email_login',
                'email_app',
                'monto_transaccion_app_30d',
                'monto_transaccion_app_60d',
                'monto_transaccion_app_90d',
                'litros_transaccion_app_30d',
                'litros_transaccion_app_60d',
                'litros_transaccion_app_90d',
                'frecuencia_transaccion_app_30d',
                'frecuencia_transaccion_app_60d',
                'frecuencia_transaccion_app_90d',
                'app_bancarizado',
                'registrado_en_app',
                'utiliza_la_app'
                ]

    def get_types_columns(self):
        return ['numeric',
                'String',
                'Date',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'String',
                'numeric',
                'numeric',
                'String',
                'String',
                'String',
                'Date',
                'String',
                'String',
                'String',
                'String',
                'String',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'String',
                'String',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'numeric',
                'String',
                'String']

    def get_dict_dtypes_columns(self):
        return {'id_cliente': 'int64',
                'genero': 'object',
                'fecha_nacimiento': 'datetime64[ns]',
                'DNI': 'object',
                'estado': 'int64',
                'permite_contacto': 'float64',
                'permite_email': 'float64',
                'permite_telefono': 'float64',
                'permite_sms': 'float64',
                'idioma': 'object',
                'es_empleado_ypf': 'object',
                'saldo_puntos': 'float64',
                'estacion_favorita1': 'object',
                'estacion_favorita2': 'object',
                'estacion_favorita3': 'object',
                'antiguedad_serviclub': 'datetime64[ns]',
                'provincia': 'object',
                'email_commarch': 'object',
                'celular': 'object',
                'telefono_fijo': 'object',
                'codigo_postal': 'object',
                'redim_30': 'float64',
                'redim_60': 'float64',
                'redim_90': 'float64',
                'redim_365': 'float64',
                'recencia_dias': 'float64',
                'frecuencia_365d': 'float64',
                'monto_boxes_30d': 'float64',
                'monto_boxes_60d': 'float64',
                'monto_boxes_90d': 'float64',
                'avg_boxes_365d': 'float64',
                'monto_infinia_30d': 'float64',
                'monto_infinia_60d': 'float64',
                'monto_infinia_90d': 'float64',
                'avg_infinia_365d': 'float64',
                'monto_infinia_diesel_30d': 'float64',
                'monto_infinia_diesel_60d': 'float64',
                'monto_infinia_diesel_90d': 'float64',
                'avg_infinia_diesel_365d': 'float64',
                'monto_super_30d': 'float64',
                'monto_super_60d': 'float64',
                'monto_super_90d': 'float64',
                'avg_super_365d': 'float64',
                'monto_ultra_30d': 'float64',
                'monto_ultra_60d': 'float64',
                'monto_ultra_90d': 'float64',
                'avg_ultra_365d': 'float64',
                'monto_gnc_30d': 'float64',
                'monto_gnc_60d': 'float64',
                'monto_gnc_90d': 'float64',
                'avg_gnc_365d': 'float64',
                'monto_tiendas_30d': 'float64',
                'monto_tiendas_60d': 'float64',
                'monto_tiendas_90d': 'float64',
                'avg_tiendas_365d': 'float64',
                'transaccion_app30d': 'float64',
                'transaccion_app60d': 'float64',
                'transaccion_app90d': 'float64',
                'transaccion_qr30d': 'float64',
                'transaccion_qr60d': 'float64',
                'transaccion_qr90d': 'float64',
                'transaccion_otros30d': 'float64',
                'transaccion_otros60d': 'float64',
                'transaccion_otros90d': 'float64',
                'socio_infinia_30d': 'int64',
                'socio_infinia_60d': 'int64',
                'socio_infinia_90d': 'int64',
                'socio_infinia_diesel_30d': 'int64',
                'socio_infinia_diesel_60d': 'int64',
                'socio_infinia_diesel_90d': 'int64',
                'socio_super_30d': 'int64',
                'socio_super_60d': 'int64',
                'socio_super_90d': 'int64',
                'socio_ultra_30d': 'int64',
                'socio_ultra_60d': 'int64',
                'socio_ultra_90d': 'int64',
                'socio_gnc_30d': 'int64',
                'socio_gnc_60d': 'int64',
                'socio_gnc_90d': 'int64',
                'email_login': 'object',
                'email_app': 'object',
                'monto_transaccion_app_30d': 'float64',
                'monto_transaccion_app_60d': 'float64',
                'monto_transaccion_app_90d': 'float64',
                'litros_transaccion_app_30d': 'float64',
                'litros_transaccion_app_60d': 'float64',
                'litros_transaccion_app_90d': 'float64',
                'frecuencia_transaccion_app_30d': 'float64',
                'frecuencia_transaccion_app_60d': 'float64',
                'frecuencia_transaccion_app_90d': 'float64',
                'app_bancarizado': 'float64',
                'registrado_en_app': 'bool',
                'utiliza_la_app': 'bool'
                }

    def check_dtypes(self, df_final):
        dtypes = self.get_dict_dtypes_columns()
        for col in df_final.columns:
            df_final[col] = df_final[col].astype(dtypes[col])
        return df_final

    def fill_numerics_na_values(self, df):
        numeric_columns = df.select_dtypes(np.number).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        return df

    def columns_to_datetime(self, df, columns):
        for col in columns:
            df[col] = pd.to_datetime(df[col])
        return df

    def add_register_app_columns(self, df):
        df['registrado_en_app'] = ~df['email_login'].isna()
        df['utiliza_la_app'] = ~df['email_app'].isna()
        return df

    def general_validation(self, df):
        df_final = df
        dates_columns = ['antiguedad_serviclub', 'fecha_nacimiento']
        for date_col in dates_columns:
            # Afuera Años que no empiecen con 1
            df_final = df_final[~df_final[date_col].astype('str').str.match('^0')]
            # Formato Fecha
            df_final = df_final[df_final['fecha_nacimiento'].astype('str').str.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}$')]

        # Genero
        # df_final = df_final[df_final['genero'].astype('str').str.match('[MFmf]')]
        return df_final

    def get_reporting_status(self, df_final, key_columns):
        """
        Function that evaluates if the filtered dataframe can be used as a final output or not
        :param df_final: dataframe with columns from both queries
        :param key_columns: list of columns that cannot have NAs. If they have NA then all future actions will be halted
        :return: report_status
        """
        # First I check if there are no NAs whatsoever
        if df_final.isnull().values.any():
            # If there are NAs, I evaluate where there are NAs: in any of the key columns or not
            if df_final[key_columns].isnull().values.any():
                # If there are any NAs in any of the key columns, we report the percentage of NAs
                report_status = 'Error'
                report_message = 'No se puede hacer ningún análisis, hay valores nulos en columnas clave.\n'
                report_message = report_message + 'A continuación, se presentan los porcentajes de NAs \n'
                for column in key_columns:
                    perc_nan = round((df_final[column].isnull().sum() / len(df_final)) * 100, 2)
                    if perc_nan > 0:
                        report_message = report_message + f"{column}: {perc_nan}% \n"
            else:
                # There are NAs but not in any of the key columns, we evaluate in which of the columns
                # there are NAs
                report_status = 'Warning'
                report_message = "Aviso. Hay nulos en la base de datos seleccionada \n"
                report_message = report_message + 'A continuación, se presentan los porcentajes de NAs \n'
                for column in list(df_final.columns):
                    perc_nan = round((df_final[column].isnull().sum() / len(df_final)) * 100, 2)
                    if perc_nan > 0:
                        report_message = report_message + f"{column}: {perc_nan}% \n"
        else:
            report_status = 'OK'
            report_message = 'Ninguna columna tiene valores nulos. \n Continuar con el análisis'
        return report_status, report_message


if __name__ == "__main__":
    print('Customer Analytics Record\n')
    car = CAR()

    car.generate_sql_connections()
    print('1. Conecciones con los servidores creadas con éxito')

    tic = time.time()
    print('2. Realizando App query...')
    df_app = car.app_query()
    toc = time.time()
    print(f"    App query Realizada - {len(df_app)} registros encontrados. Tiempo: {round(toc - tic, 2)} seg.")

    tic_f = time.time()
    print('3. Realizando Comarch query...')

    car.azure_conn.execute(f"DELETE FROM CAR")
    car.azure_conn.commit()
    chunksize = 500000
    for df_commarch in tqdm(pd.io.sql.read_sql(car.commarch_query(), con=car.comarch_conn, chunksize=chunksize)):
        # Merging App
        df_final, report_message = car.merge_commarch_app(df_commarch, df_app)

        # Cleaning Data
        df_final = car.general_validation(df_final)
        df_final = car.check_dtypes(df_final)
        df_final = car.fill_numerics_na_values(df_final)
        df_final = car.columns_to_datetime(df_final, ['fecha_nacimiento', 'antiguedad_serviclub'])
        df_final = car.add_register_app_columns(df_final)

        print('chunksize')

        # To Azure
        n = 1000
        for i in range(0, df_final.shape[0], n):
            car.insert_new_data(df=df_final[i:i + n],
                                table_name='dbo.CAR',
                                columns_types=car.get_types_columns(),
                                conn=car.azure_conn,
                                mode=2)
            print('     insert')
        car.azure_conn.commit()
        car.generate_sql_connections()

    car.azure_conn.execute(f"DELETE FROM update_date_CAR")
    car.azure_conn.execute(f"INSERT INTO dbo.update_date_CAR VALUES ('{dt.datetime.today().date()}')")
    car.azure_conn.commit()

    toc = time.time()
    print(f"    Subida de CAR realizada. Tiempo: {round((toc - tic_f) / 60, 2)} min.")

    print("\nPrograma Finalizado!")

    """tic = time.time()
    print('4. Uniendo Comarch y App queries...')
    df_final, report_message = car.merge_commarch_app(df_commarch,df_app)
    toc = time.time()
    print(f'    {report_message}. Tiempo: {round(toc-tic,2)} seg.')

    tic = time.time()
    print('5. Procesando la tabla final...')
    df_final = car.general_validation(df_final)
    df_final = car.check_dtypes(df_final)
    df_final = car.fill_numerics_na_values(df_final)
    df_final = car.columns_to_datetime(df_final, ['fecha_nacimiento', 'antiguedad_serviclub'])
    df_final = car.add_register_app_columns(df_final)
    #df_final.to_csv('car.csv')
    toc = time.time()
    print(f'    Procesado Finalizado. Tiempo: {round(toc-tic,2)} seg.')

    #report_status, report_message = car.get_reporting_status(df_final)
    #print(report_message)
    tic = time.time()
    print('6. Subiendo CAR a la base de datos...')
    car.azure_conn.execute(f"DELETE FROM CAR")
    n = 1000  # chunk row size
    list_df = [df_final[i:i + n] for i in range(0, df_final.shape[0], n)]
    total_iteration, i = round(df_final.shape[0]/n,0), 0
    df_final = None
    for df in tqdm(list_df):
        car.insert_new_data(df=df,
                        table_name='dbo.CAR',
                        columns_types=car.get_types_columns(),
                        conn=car.azure_conn)
        # Sube a base de datos cada 100.000 registros
        if i%100==0:
            car.azure_conn.commit()
        i+=1
    car.azure_conn.commit()
    # Updating Date
    car.azure_conn.execute(f"DELETE FROM update_date_CAR")
    car.azure_conn.execute(f"INSERT INTO dbo.update_date_CAR VALUES ('{dt.datetime.today().date()}')")
    car.azure_conn.commit()
    toc = time.time()
    print(f'    Subida Finalizada. Tiempo: {round((toc-tic)/60,2)} min.')

    print("\nPrograma Finalizado!")
    time.sleep(5)"""