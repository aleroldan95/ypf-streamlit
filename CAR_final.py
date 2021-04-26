import streamlit as st
import datetime as dt
import os
import json
import pyodbc
import inspect
import sys

import pandas as pd
import base64
import plotly.graph_objects as go
import pathlib

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
from st_classes import *

def st_main():
    # Configuraciones generales------------
    st.set_page_config(page_title='CAR',
                       page_icon=None,
                       layout='centered',
                       initial_sidebar_state='auto')

    state = get_state()

    if state.is_first_state is None:
        state.key = 1
        clean_state(state)

    state.style_class = Style()
    state.style_class.set_style()

    # SideBar------------------------------
    state.style_class.ypf_side_bar()
    state.style_class.ag_sidebar_title("Customer Analytics Record")
    st.sidebar.markdown("---")

    pages = {'Demográficas/Contacto/Product Drive': contact,
             'Programa de Fidelidad y Hábitos': fidelity,
             'Información de Compra': purchase_info,
             'Métodos de Pago': payments_types}
    state.style_class.ag_siderbar("Seleccioná una página")
    page = st.sidebar.selectbox("", tuple(pages.keys()))

    st.sidebar.markdown("---")
    state.style_class.ag_siderbar("Porcentaje de la base alcanzada")
    kpi_plot = st.sidebar.empty()
    kpi_plot.plotly_chart(generate_kpi_chart(state.query.shape[0], state.original.iloc[0,0]))
    download_option = st.sidebar.empty()
    st.sidebar.markdown("---")
    if st.sidebar.button("Reiniciar Filtros"):
        clean_state(state)

    if st.sidebar.button('<Correr Query>'):
        azure_conn = server_connection(os.getcwd(), 'config_azure_SQLCU')
        state.original = pd.io.sql.read_sql('select count(*) from CAR', azure_conn)
        state.query = big_query(state)
        kpi_plot.plotly_chart(generate_kpi_chart(state.query.shape[0], state.original.iloc[0,0]))
        df = state.query[~(state.query.email_commarch.isnull() & state.query.email_login.isnull())]
        df["id_cliente"] = df["id_cliente"].astype('int')
        show_dowload_option(df, f'query_car', download_option=download_option)

    #last_date = pd.io.sql.read_sql('select * from update_date_CAR', server_connection(os.getcwd(), 'config_azure_SQLCU'))
    #state.style_class.ag_sidebar_date(f'Última Actualización: {last_date.iloc[0,0]}')

    pages[page](state)


def contact(state):
    state.style_class.ag("Customer Analytics Record")

    state.style_class.ag1("Demográficas", "perm_identity")
    state.style_class.ag2("Género")
    cols = st.beta_columns(4)
    state.is_m = cols[0].checkbox('Masculino', value=state.is_m, key=f'a{state.key}')
    state.is_f = cols[1].checkbox('Femenino', value=state.is_f, key=f'b{state.key}')

    state.style_class.ag2("Edad")
    state.style_class.ag3("Seleccione rango")
    cols = st.beta_columns([1, 5, 1])  #

    state.min_max_range['age_range']['min'] = cols[0].text_input('', value=int(
        state.min_max_range['age_range']['min']), max_chars=None, key=f'g45{state.key}')
    state.min_max_range['age_range']['max'] = cols[2].text_input('', value=int(
        state.min_max_range['age_range']['max']), max_chars=None, key=f'h3424{state.key}')
    validate_int_numbers(state, 'age_range')
    state.age_range = cols[1].slider('', 0, 100, (int(state.min_max_range['age_range']['min']),
                                                  int(state.min_max_range['age_range']['max'])), 1, key=f'c{state.key}')

    state.style_class.ag2("Región")
    state.style_class.ag3("Seleccione Región")
    state.province = st.multiselect('',
                                   options= ['Todas las Regiones', 'NULL', '<unknown>', 'Buenos Aires', 'Capital Federal', 'Catamarca', 'Chaco',
                                   'Chubut', 'Cordoba', 'Corrientes', 'Entre Rios', 'Formosa', 'Jujuy', 'La Pampa',
                                   'La Rioja', 'Mendoza', 'Misiones', 'Neuquen', 'Rio Negro', 'Salta', 'San Juan',
                                   'San Luis', 'Santa Cruz', 'Santa Fe', 'Santiago Del Estero', 'Tierra Del Fuego',
                                   'Tucuman'],
                                   default=state.province,
                                    key=f'd{state.key}')

    state.style_class.ag2("Empleado YPF")
    cols = st.beta_columns(4)
    state.is_employee = cols[0].checkbox('Sí', value=state.is_employee, key=f'e{state.key}')
    state.is_not_employee = cols[1].checkbox('No', value=state.is_not_employee, key=f'f{state.key}')

    state.style_class.ag2("Código Postal")
    state.style_class.ag3('Ingresar lista en formato CSV separado por comas en una única columna. Sin nombre de columna')
    df_pickle = st.file_uploader('',type=('csv'), key=f'{state.key}')
    if df_pickle:
        #df_pickle.seek(0)
        df_cod_postal = pd.read_csv(df_pickle, sep=',',  header=None)
        df_postal_code = generate_list_of_postal_code()
        if df_cod_postal.iloc[:,0].astype('str').isin(list(df_postal_code.CCD_PostalCode)).sum() != df_cod_postal.shape[0]:
            st.warning("Se ingresaron códigos postales no incluidos en la columna CCD_PostalCode de DIM_ContactDetails")
        else:
            st.success('Los códigos postales ingresados fueron encontrados en la base de datos')
        state.cod_postal = list(df_cod_postal.iloc[:,0])

    state.style_class.ag1("Contacto", "stay_current_portrait")
    state.style_class.ag3("Permite")
    contact_types = ['Contacto', 'Email', 'Teléfono', 'SMS']
    aux = {'Si':0, 'No':1, 'Sin Filtrar':2}
    cols = st.beta_columns(4)
    for i, contact in enumerate(contact_types):
        state.style_class.ag2(contact, cols[i])
        aux_contact = {}
        aux_contact['permite'] = cols[i].checkbox("Sí", value=state.contact_type[contact]['permite'], key=f'{contact}si{state.key}')
        aux_contact['no_permite'] = cols[i].checkbox("No", value=state.contact_type[contact]['no_permite'], key=f'{contact}no{state.key}')
        state.contact_type[contact] = aux_contact
        #state.contact_type[contact] = cols[i].radio("",
        #                                            options=['Si', 'No', 'Sin Filtrar'],
        #                                            index=aux[state.contact_type[contact]],
        #                                            key=contact)

    state.style_class.ag1("Product driver", "local_gas_station")
    state.style_class.ag3("Tipo de Socio")
    aux_2 = {'Si': 0, 'No': 1, 'Sin Filtrar': 2, 30: 0, 60: 1, 90: 2}
    cols = st.beta_columns(5)
    types = ['Infinia', 'Infinia Diesel', 'Super', 'Ultra', 'GNC']
    for i, ty in enumerate(types):
        aux = {}
        state.style_class.ag2(ty, cols[i])
        cols[i].write("")
        aux['permite'] = cols[i].checkbox("Si", value=state.socio_type[ty]['permite'], key=f'{ty}si{state.key}')
        aux['no_permite'] = cols[i].checkbox("No", value=state.socio_type[ty]['no_permite'], key=f'{ty}no{state.key}')
        aux['period'] = cols[i].selectbox("Período",
                                          options=[30, 60, 90],
                                          index=aux_2[state.socio_type[ty]['period']],
                                          key=f'socio_{ty}{state.key}')
        state.socio_type[ty] = aux

def fidelity(state):
    state.style_class.ag("Customer Analytics Record")

    state.style_class.ag1("Programa de Fidelidad", "star_border")
    state.style_class.ag2("Saldo de Puntos")
    state.style_class.ag3("Ingrese Rango")
    cols = st.beta_columns([1,5,1]) #
    state.min_max_range['points_range']['min'] = cols[0].text_input('', value=int(state.min_max_range['points_range']['min']), max_chars=None, key=f'g{state.key}')
    state.min_max_range['points_range']['max'] = cols[2].text_input('', value=int(state.min_max_range['points_range']['max']), max_chars=None, key=f'h{state.key}')
    validate_int_numbers(state, 'points_range')
    state.points_range = cols[1].slider('', 0, 150000, (int(state.min_max_range['points_range']['min']), int(state.min_max_range['points_range']['max'])), 1000, key=f'w{state.key}')

    state.style_class.ag2('Estado')
    cols = st.beta_columns(4)
    state.active = cols[0].checkbox('Activo', value=state.active, key=f'i{state.key}')
    state.not_active = cols[1].checkbox('Inactivo', value=state.not_active, key=f'j{state.key}')

    state.style_class.ag2('Estado en la App')
    cols = st.beta_columns(3)
    state.style_class.ag3('Registrado en la App', cols[0])
    state.style_class.ag3('Utiliza la App', cols[1])
    state.style_class.ag3('Bancarizado en App', cols[2])
    cols = st.beta_columns(6)
    state.is_in_app = cols[0].checkbox('Sí', value=state.is_in_app, key=f'k{state.key}')
    state.is_not_in_app = cols[1].checkbox('No', value=state.is_not_in_app, key=f'l{state.key}')
    state.is_using_app = cols[2].checkbox('Sí', value=state.is_using_app, key=f'm{state.key}')
    state.is_not_using_app = cols[3].checkbox('No', value=state.is_not_using_app, key=f'n{state.key}')
    state.is_banc_app = cols[4].checkbox('Sí', value=state.is_banc_app, key=f'sdsm{state.key}')
    state.is_not_banc_app = cols[5].checkbox('No', value=state.is_not_banc_app, key=f'nsdads{state.key}')

    state.style_class.ag2('Puntos redimidos')
    cols = st.beta_columns([1, 1, 4, 1])
    state.style_class.ag3("Período", cols[0])
    aux = {30:0, 60:1, 90:2, 365:3}
    state.redim_period = cols[0].selectbox("", options=[30, 60, 90, 365], index=aux[state.redim_period], key=f'o{state.key}')
    state.style_class.ag3("", cols[1])
    state.style_class.ag3("", cols[3])
    state.min_max_range['redim_range']['min'] = cols[1].text_input('',
                                                                   value=int(state.min_max_range['redim_range']['min']),
                                                                   max_chars=None, key=f'redim{state.key}')
    state.min_max_range['redim_range']['max'] = cols[3].text_input('',
                                                                   value=int(state.min_max_range['redim_range']['max']),
                                                                   max_chars=None, key=f'redim2{state.key}')
    validate_int_numbers(state, 'redim_range')
    state.style_class.ag3("Monto", cols[2])
    state.redim_range =  cols[2].slider('', 0, 300000,
                                        (int(state.min_max_range['redim_range']['min']), int(state.min_max_range['redim_range']['max'])),
                                        1000, key=f'p{state.key}')


    state.style_class.ag1("Hábitos", "watch_later")
    cols = st.beta_columns(2)
    df_list_of_stations = generate_list_of_stations()
    list_of_stations = list(df_list_of_stations.ACCC_TheMostFavouriteSite)
    state.style_class.ag3("Estación Favorita 1", cols[0])
    state.stations1 = cols[0].multiselect("", options=list_of_stations, default=state.stations1, key=f'r{state.key}')
    state.style_class.ag3("Estación Favorita 2", cols[1])
    state.stations2 = cols[1].multiselect("", options=list_of_stations, default=state.stations2, key=f's{state.key}')

    state.style_class.ag2("Antiguedad Serviclub")
    state.style_class.ag3("Ingrese rango (en años)")
    cols = st.beta_columns([1, 5, 1])  #
    state.min_max_range['ant_serviclub']['min'] = cols[0].text_input('', value=int(
        state.min_max_range['ant_serviclub']['min']), max_chars=None, key=f'g145{state.key}')
    state.min_max_range['ant_serviclub']['max'] = cols[2].text_input('', value=int(
        state.min_max_range['ant_serviclub']['max']), max_chars=None, key=f'h34124{state.key}')
    validate_int_numbers(state, 'ant_serviclub')
    state.ant_serviclub = cols[1].slider('', 0,50, (int(state.min_max_range['ant_serviclub']['min']),
                                               int(state.min_max_range['ant_serviclub']['max'])), 1, key=f't{state.key}')


def purchase_info(state):
    state.style_class.ag("Customer Analytics Record")

    state.style_class.ag1("Información de Compras", "credit_card")
    types_comb = ['Infinia', 'Super', 'Infinia Diesel', 'Ultra', 'GNC', 'Boxes', 'Tiendas']
    aux2 = {30: 0, 60: 1, 90: 2, 365: 3}
    for comb in types_comb:
        state.style_class.ag2(comb)
        cols = st.beta_columns([1, 1, 4, 1])  #
        aux = {}
        state.style_class.ag3("Período", cols[0])
        state.style_class.ag3("", cols[1])
        state.style_class.ag3("", cols[3])
        state.min_max_range['st_variables_comb'][comb]['min'] = cols[1].text_input('', value=int(
            state.min_max_range['st_variables_comb'][comb]['min']), max_chars=None, key=f'as23d{comb}{state.key}')
        state.min_max_range['st_variables_comb'][comb]['max'] = cols[3].text_input('', value=int(
            state.min_max_range['st_variables_comb'][comb]['max']), max_chars=None, key=f'dd31d{comb}{state.key}')
        validate_int_numbers(state, 'st_variables_comb', comb)

        aux['period'] = cols[0].selectbox("",
                                          options=[30, 60, 90],
                                          index=aux2[state.st_variables_comb[comb]['period']],
                                          key=f'u{comb}{state.key}')
        state.style_class.ag3("Monto en el período seleccionado", cols[2])
        aux['amount'] = cols[2].slider('', 0, 3000000, (int(state.min_max_range['st_variables_comb'][comb]['min']),
                                                        int(state.min_max_range['st_variables_comb'][comb]['max'])),
                                       10000, key=f'{comb}{state.key}')
        state.style_class.ag3("Monto Promedio 365 días")
        cols = st.beta_columns([1, 5, 1])
        state.min_max_range['st_variables_comb_avg'][comb]['min'] = cols[0].text_input('', value=int(
            state.min_max_range['st_variables_comb_avg'][comb]['min']), max_chars=None, key=f'g4a2s5{comb}{state.key}')
        state.min_max_range['st_variables_comb_avg'][comb]['max'] = cols[2].text_input('', value=int(
            state.min_max_range['st_variables_comb_avg'][comb]['max']), max_chars=None, key=f'ha2{comb}{state.key}')
        validate_int_numbers(state, 'st_variables_comb_avg', comb)

        aux['avg'] = cols[1].slider('', 0, 1000000, (int(state.min_max_range['st_variables_comb_avg'][comb]['min']),
                                                     int(state.min_max_range['st_variables_comb_avg'][comb]['max'])), 1000,
                                    key=f'2{comb}{state.key}')
        # generate_space(cols[3], 8)
        state.st_variables_comb[comb] = aux

    state.style_class.ag1("Hábitos de Compras", "watch_later")
    state.style_class.ag2('Recencia')
    cols = st.beta_columns([1,5,1])
    state.min_max_range['rec']['min'] = cols[0].text_input('', value=int(state.min_max_range['rec']['min']), max_chars=None, key=f'gsgg{state.key}')
    state.min_max_range['rec']['max'] = cols[2].text_input('', value=int(state.min_max_range['rec']['max']), max_chars=None, key=f'hasd{state.key}')
    validate_int_numbers(state, 'rec')
    state.rec = cols[1].slider('', 0, 2000, (int(state.min_max_range['rec']['min']),
                                        int(state.min_max_range['rec']['max'])), 10, key=f'rec{state.key}', format="%d días")
    state.style_class.ag2('Frecuencia (365 días)')
    cols = st.beta_columns([1, 5, 1])
    state.min_max_range['freq']['min'] = cols[0].text_input('', value=int(state.min_max_range['freq']['min']),
                                                           max_chars=None, key=f'gasdsgg{state.key}')
    state.min_max_range['freq']['max'] = cols[2].text_input('', value=int(state.min_max_range['freq']['max']),
                                                           max_chars=None, key=f'hasdasd{state.key}')
    validate_int_numbers(state, 'freq')
    state.freq = cols[1].slider('', 0, 2000, (int(state.min_max_range['freq']['min']),
                                              int(state.min_max_range['freq']['max'])), 10, key=f'{state.key}freq')

    state.style_class.ag1("Utilización de App", 'stay_current_portrait')
    app_types = ['Transacción', 'Litros', 'Frecuencia']
    aux2 = {30: 0, 60: 1, 90: 2}
    for comb in app_types:
        state.style_class.ag2(comb)
        cols = st.beta_columns([1, 1, 4, 1])
        state.style_class.ag3("Período", cols[0])
        state.st_variables_app[comb]['period'] = cols[0].selectbox("", options=[30, 60, 90], index=aux2[state.st_variables_app[comb]['period']], key=f'{comb}{state.key}')
        state.style_class.ag3("", cols[1])
        state.style_class.ag3("", cols[3])

        state.min_max_range['st_variables_app'][comb]['min'] = cols[1].text_input('', value=int(
            state.min_max_range['st_variables_app'][comb]['min']), max_chars=None, key=f'ajsd{comb}{state.key}')
        state.min_max_range['st_variables_app'][comb]['max'] = cols[3].text_input('', value=int(
            state.min_max_range['st_variables_app'][comb]['max']), max_chars=None, key=f'ddjd{comb}{state.key}')

        validate_int_numbers(state, 'st_variables_app', comb)
        state.style_class.ag3("Monto", cols[2])
        state.st_variables_app[comb]['amount'] = cols[2].slider('',
                                       0,
                                       state.st_variables_app[comb]['max'],
                                      (int(state.min_max_range['st_variables_app'][comb]['min']),
                                       int(state.min_max_range['st_variables_app'][comb]['max'])),
                                       state.min_max_range['st_variables_app'][comb]['freq'],
                                       key=f'po{comb}{state.key}')



def payments_types(state):
    state.style_class.ag("Customer Analytics Record")

    state.style_class.ag1("Métodos de Pago", 'credit_card')
    types_payments = ['QR', 'App', 'Otros']
    aux2 = {30: 0, 60: 1, 90: 2}
    for comb in types_payments:
        state.style_class.ag2(comb)
        cols = st.beta_columns([1,1,4,1]) #
        aux = {}
        state.style_class.ag3("Período", cols[0])
        aux['period'] = cols[0].selectbox("", options=[30, 60, 90], index= aux2[state.st_variables_payment[comb]['period']], key=f'{comb}{state.key}')
        state.style_class.ag3("", cols[1])
        state.style_class.ag3("", cols[3])
        state.min_max_range['st_variables_payment'][comb]['min'] = cols[1].text_input('', value=int(
            state.min_max_range['st_variables_payment'][comb]['min']), max_chars=None, key=f'asd{comb}{state.key}')
        state.min_max_range['st_variables_payment'][comb]['max'] = cols[3].text_input('', value=int(
            state.min_max_range['st_variables_payment'][comb]['max']), max_chars=None, key=f'ddd{comb}{state.key}')
        validate_int_numbers(state, 'st_variables_payment', comb)

        state.style_class.ag3("Monto", cols[2])
        aux['amount'] = cols[2].slider('', 0, 1000000, (int(state.min_max_range['st_variables_payment'][comb]['min']),
                                                        int(state.min_max_range['st_variables_payment'][comb]['max'])),
                                       10000, key=f'{comb}{state.key}')
        state.st_variables_payment[comb] = aux

def server_connection(execution_path, config_name):
    settings = json.load(open(f'{execution_path}\\credenciales\\{config_name}.json'))
    # Error fixed
    settings["DRIVER"] = "SQL Server"
    conn = pyodbc.connect(
        f'DRIVER={settings["DRIVER"]};SERVER={settings["SERVER"]};DATABASE={settings["DATABASE"]};UID={settings["UID"]};PWD={settings["PWD"]}')
    return conn

def big_query(state):

    query = "select id_cliente, email_commarch, email_login from dbo.CAR where "

    # Genero
    if state.is_m and not state.is_f: query += 'genero=\'M\' and '
    elif state.is_f and not state.is_m:
        query += 'genero=\'F\' and '
    elif state.is_m and state.is_f:
        query += '(genero=\'F\' or genero=\'M\') and '
    else:
        pass
        #st.sidebar.warning("Filtro de Género Sin Utilizar")

    #Edad
    query += f'DATEDIFF(yy , fecha_nacimiento, sysdatetime()) between {state.age_range[0]} and {state.age_range[1]} and '

    # Antiguedad Serviclub
    query += f'DATEDIFF(yy ,antiguedad_serviclub , sysdatetime()) between {state.ant_serviclub[0]} and {state.ant_serviclub[1]} and '

    # Tipo de Empleado
    if state.is_employee and not state.is_not_employee:
        query += 'es_empleado_ypf=1 and '
    elif not state.is_employee and state.is_not_employee:
        query += 'es_empleado_ypf<> 1 and '
    elif state.is_employee and state.is_not_employee:
        pass
    else:
        pass
        #st.sidebar.warning("Filtro de Estado No Utilizado")

    # Status
    if state.active and not state.not_active:
        query += 'estado= 1 and '
    elif not state.active and state.not_active:
        query += 'estado<> 1 and '

    # Registrado en App
    if state.is_in_app and not state.is_not_in_app:
        query += 'registrado_en_app = \'True\' and '
    elif not state.is_in_app and state.is_not_in_app:
        query += 'registrado_en_app = \'False\' and '

    if state.is_using_app and not state.is_not_using_app:
        query += 'utiliza_la_app = \'True\' and '
    elif not state.is_using_app and state.is_not_using_app:
        query += 'utiliza_la_app = \'False\' and '

    if state.is_banc_app and not state.is_not_banc_app:
        query += 'app_bancarizado = 1 and '
    elif not state.is_banc_app and state.is_not_banc_app:
        query += 'app_bancarizado = 0 and '

    # Contacto
    contact_types = ['Contacto', 'Email', 'Teléfono', 'SMS']
    aux = {'Contacto':'permite_contacto', 'Email': 'permite_email', 'Teléfono': 'permite_telefono', 'SMS': 'permite_sms'}
    for contact in contact_types:
        if state.contact_type[contact]['permite'] and not state.contact_type[contact]['no_permite']:
            query += f'{aux[contact]} = 1 and '
        elif not state.contact_type[contact]['permite'] and state.contact_type[contact]['no_permite']:
            query += f'{aux[contact]} = 0 and '
        elif state.contact_type[contact]['permite'] and state.contact_type[contact]['no_permite']:
            query += f'({aux[contact]} = 1 or {aux[contact]} = 0) and '

    # Saldo Puntos
    query += f'saldo_puntos between {state.points_range[0]} and {state.points_range[1]} and '

    # Estaciones Favoritas
    if state.stations1:
        aux = '('
        for i in state.stations1:
            aux += '\'' + str(i) + '\', '
        aux += '\'\')'
        query += f'estacion_favorita1 in {aux} and '
    if state.stations2:
        aux = '('
        for i in state.stations2:
            aux += '\'' + str(i) + '\', '
        aux += '\'\')'
        query += f'estacion_favorita1 in {aux} and '

    # Provincia
    if state.province and state.province!=['Todas las Regiones']:
        aux = '('
        if 'NULL' in state.province:
            state.province.remove('NULL')
            aux += '\'nan\', '
        for i in state.province:
            aux += '\'' + str(i) + '\', '
        aux += '\'\')'
        query += f'provincia in {aux} and '

    #Código Postal
    if state.cod_postal:
        aux = '('
        for i in state.cod_postal:
            aux += '\'' + str(i) + '\', '
        aux += '\'\')'
        query += f'codigo_postal in {aux} and '

    # Puntos Redimidos
    query += f'redim_{state.redim_period} between {state.redim_range[0]} and {state.redim_range[1]} and '

    # Recencia y Frecuencia
    query += f'recencia_dias between {state.rec[0]} and {state.rec[1]} and '
    query += f'frecuencia_365d between {state.freq[0]} and {state.freq[1]} and '

    # Información de Compras
    types_comb = ['Infinia', 'Super', 'Infinia Diesel', 'Ultra', 'GNC', 'Boxes', 'Tiendas']
    for comb in types_comb:
        period = state.st_variables_comb[comb]['period']
        min = state.st_variables_comb[comb]['amount'][0]
        max = state.st_variables_comb[comb]['amount'][1]
        avg = state.st_variables_comb[comb]['avg']
        if comb == 'Infinia Diesel': comb = 'infinia_diesel'
        column = 'monto_' + comb.lower() + f'_{period}d'
        avg_column = 'avg_' + comb.lower() + '_365d'
        query += f'{column} between {min} and {max} and '
        query += f'{avg_column} between {avg[0]} and {avg[1]} and '

    # Tipo de Socio
    types = ['Infinia', 'Infinia Diesel', 'Super', 'Ultra', 'GNC']
    for ty in types:
        period = state.socio_type[ty]['period']
        if state.socio_type[ty]['permite'] and not state.socio_type[ty]['no_permite']:
            if ty == 'Infinia Diesel': ty = 'infinia_diesel'
            column_name = 'socio_' + ty.lower() + f'_{period}d'
            query += f'{column_name} = 1 and '
        elif not state.socio_type[ty]['permite'] and state.socio_type[ty]['no_permite']:
            if ty == 'Infinia Diesel': ty = 'infinia_diesel'
            column_name = 'socio_' + ty.lower() + f'_{period}d'
            query += f'{column_name} = 0 and '

    # Utilización de la App
    app_types = ['Transacción', 'Litros', 'Frecuencia']
    for ty in app_types:
        period = state.st_variables_app[ty]['period']
        monto = state.st_variables_app[ty]['amount']
        if ty == 'Transacción': ty = 'monto'
        column = ty.lower() + '_transaccion_app_' + f'{period}d'
        query += f'{column} between {monto[0]} and {monto[1]} and '

    # Métodos de Pago
    types_payments = ['QR', 'App', 'Otros']
    for comb in types_payments:
        period = state.st_variables_payment[comb]['period']
        min = state.st_variables_payment[comb]['amount'][0]
        max = state.st_variables_payment[comb]['amount'][1]
        column = 'transaccion_' + comb.lower() + f'{period}d'
        query += f'{column} between {min} and {max} and '

    query += '1=1'
    return pd.io.sql.read_sql(query, con=server_connection(os.getcwd(), 'config_azure_SQLCU'))


def clean_state(state):
    state.key += 1

    state.min_max_range_default = {'points_range': {'min': '0', 'max': '150000'},
                                    'redim_range': {'min': '0', 'max': '300000'},
                                   'rec': {'min': '0', 'max': '2000'},
                                   'freq': {'min': '0', 'max': '2000'},
                                   'age_range': {'min': '0', 'max': '100'},
                                   'ant_serviclub': {'min': '0', 'max': '50'},
                                   'st_variables_payment': {'QR': {'min': '0', 'max': '1000000'},
                                                            'App': {'min': '0', 'max': '1000000'},
                                                            'Otros': {'min': '0', 'max': '1000000'}},
                                   'st_variables_comb': {'Infinia': {'min': '0', 'max': '3000000'},
                                                         'Infinia Diesel': {'min': '0', 'max': '3000000'},
                                                         'Super': {'min': '0', 'max': '3000000'},
                                                         'Ultra': {'min': '0', 'max': '3000000'},
                                                         'GNC': {'min': '0', 'max': '3000000'},
                                                         'Boxes': {'min': '0', 'max': '3000000'},
                                                         'Tiendas': {'min': '0', 'max': '3000000'}},
                                   'st_variables_comb_avg': {'Infinia': {'min': '0', 'max': '1000000'},
                                                         'Infinia Diesel': {'min': '0', 'max': '1000000'},
                                                         'Super': {'min': '0', 'max': '1000000'},
                                                         'Ultra': {'min': '0', 'max': '1000000'},
                                                         'GNC': {'min': '0', 'max': '1000000'},
                                                         'Boxes': {'min': '0', 'max': '1000000'},
                                                         'Tiendas': {'min': '0', 'max': '1000000'}},
                                   'st_variables_app': {'Transacción': {'period': 30, 'min': '0', 'max': '700000', 'freq': 10000},
                                                        'Litros': {'period': 30, 'min': '0', 'max': '7000', 'freq': 100},
                                                        'Frecuencia': {'period': 30, 'min': '0', 'max': '200', 'freq': 10}}
                                   }
    state.min_max_range = {'points_range': {'min': '0', 'max': '150000'},
                           'redim_range': {'min': '0', 'max': '300000'},
                           'rec':{'min': '0', 'max': '2000'},
                           'freq': {'min': '0', 'max': '2000'},
                           'age_range': {'min': '0', 'max': '100'},
                           'ant_serviclub': {'min': '0', 'max': '50'},
                           'st_variables_payment': {'QR':{'min': '0', 'max':'1000000'},
                                                    'App':{'min': '0', 'max':'1000000'},
                                                    'Otros':{'min': '0', 'max':'1000000'}},
                           'st_variables_comb': {'Infinia': {'min': '0', 'max': '3000000'},
                                                 'Infinia Diesel': {'min': '0', 'max': '3000000'},
                                                 'Super': {'min': '0', 'max': '3000000'},
                                                 'Ultra': {'min': '0', 'max': '3000000'},
                                                 'GNC': {'min': '0', 'max': '3000000'},
                                                 'Boxes': {'min': '0', 'max': '3000000'},
                                                 'Tiendas': {'min': '0', 'max': '3000000'}},
                           'st_variables_comb_avg': {'Infinia': {'min': '0', 'max': '1000000'},
                                                     'Infinia Diesel': {'min': '0', 'max': '1000000'},
                                                     'Super': {'min': '0', 'max': '1000000'},
                                                     'Ultra': {'min': '0', 'max': '1000000'},
                                                     'GNC': {'min': '0', 'max': '1000000'},
                                                     'Boxes': {'min': '0', 'max': '1000000'},
                                                     'Tiendas': {'min': '0', 'max': '1000000'}},
                           'st_variables_app': {
                               'Transacción': {'period': 30, 'min': '0', 'max': '700000', 'freq': 10000},
                               'Litros': {'period': 30, 'min': '0', 'max': '7000', 'freq': 100},
                               'Frecuencia': {'period': 30, 'min': '0', 'max': '200', 'freq': 10}}
                           }

    state.is_first_state = False
    state.original = pd.DataFrame({'hola': [1]})
    state.query = pd.DataFrame({'hola': [1]})

    state.is_m = False
    state.is_f = False
    state.age_range = (0, 100)
    state.is_region_filtered = False
    state.province = ['Todas las Regiones']
    state.cod_postal = []
    state.is_employee = False
    state.is_not_employee = False

    contact_types = ['Contacto', 'Email', 'Teléfono', 'SMS']
    state.contact_type = {}
    for contact in contact_types:
        state.contact_type[contact] = {'permite': False, 'no_permite': False}

    state.is_infinia = False
    state.is_super = False
    state.is_diesel = False
    state.is_ultra = False
    state.is_gnc = False
    state.socio_range = 60

    state.points_range = (0, 150000)

    state.active = False
    state.not_active = False
    state.is_in_app = False
    state.is_not_in_app = False
    state.is_using_app = False
    state.is_not_using_app = False
    state.is_banc_app = False
    state.is_not_banc_app = False

    state.redim_period = 30
    state.redim_range = (0, 200000)

    state.socio_period = 30
    state.socio_type = {}
    types = ['Infinia', 'Infinia Diesel', 'Super', 'Ultra', 'GNC']
    for ty in types:
        aux = {'period': 30, 'permite': False, 'no_permite': False}
        state.socio_type[ty] = aux

    state.stations1 = []
    state.stations2 = []
    state.list_of_available_stations = None
    state.ant_serviclub = (0, 50)

    types_comb = ['Infinia', 'Super', 'Infinia Diesel', 'Ultra', 'GNC', 'Boxes', 'Tiendas']
    state.st_variables_comb = {}
    for comb in types_comb:
        aux = {'period': 30, 'amount': (0, 3000000), 'avg': (0, 1000000)}
        state.st_variables_comb[comb] = aux

    state.rec = (0, 2000)
    state.freq = (0, 2000)

    app_types = ['Transacción', 'Litros', 'Frecuencia']
    state.st_variables_app = {}
    state.st_variables_app['Transacción'] = {'period': 30, 'amount': (0, 700000), 'max': 700000, 'freq': 10000}
    state.st_variables_app['Litros'] = {'period': 30, 'amount': (0, 7000), 'max': 7000, 'freq': 100}
    state.st_variables_app['Frecuencia'] = {'period': 30, 'amount': (0, 200), 'max': 200, 'freq': 10}

    types_payments = ['QR', 'App', 'Otros']
    state.st_variables_payment = {}
    for comb in types_payments:
        aux = {'period': 30, 'amount': (0, 1000000)}
        state.st_variables_payment[comb] = aux


def generate_kpi_chart(val, total):

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=val/total*100,
        #title={'text': "Porcentaje de la Base Alcanzada"},
        domain={'x': [0, 1], 'y': [0, 1]},
        gauge={'axis': {'range': [None, 100], 'tickwidth': 1},
               'bgcolor': '#FFFFFF',
               'bar': {'color': "#4399FF"},
               'borderwidth': 0}
    ))
    fig.update_layout(margin=go.layout.Margin(
                          l=0,  # left margin
                          r=0,  # right margin
                          b=10,  # bottom margin
                          t=25  # top margin
                      ),
                      height=120,
                      plot_bgcolor='grey',
                        width=230,
                        paper_bgcolor="#724AE2",
                        font={'color': "#FFFFFF", 'family': "Montserrat"}
                      )
    fig.update_traces(number_suffix=' %', selector=dict(type='indicator'))
    fig.update_traces(gauge_axis_tickfont_size= 8, selector = dict(type='indicator'))

    return fig

def show_dowload_option(df, name, download_option=None):
    style = """text-decoration:none; 
                        background-color:#ffffff; 
                        padding-top: 10px; padding-bottom: 10px; padding-left: 25px; padding-right: 25px;
                        box-shadow: 4px 4px 14px rgba(0, 0, 0, 0.25); 
                        font-family: Montserrat;
                        font-style: normal;
                        font-weight: 600;
                        font-size: 11px;
                        line-height: 13px;
                        text-align: center;
                        color: #FF037C;"""
    STREAMLIT_STATIC_PATH = pathlib.Path(st.__path__[0]) / 'static'
    DOWNLOADS_PATH = (STREAMLIT_STATIC_PATH / "downloads")
    if not DOWNLOADS_PATH.is_dir():
        DOWNLOADS_PATH.mkdir()

    if os.path.exists(str(DOWNLOADS_PATH / f"{name}.xlsx")):
        os.remove(str(DOWNLOADS_PATH / f"{name}.xlsx"))

    df.to_csv(str(DOWNLOADS_PATH / f"{name}.csv"), index=False)

    download_option.markdown(
        f'<a href="downloads/{name}.csv" download="{name}.csv" style="{style}">Descargar Archivo CSV</a>',
        unsafe_allow_html=True)

def get_binary_file_downloader_html(bin_file, file_label='File'):
    with open(bin_file, 'rb') as f:
        data = f.read()
    bin_str = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{bin_str}" download="{os.path.basename(bin_file)}"><input type="button" value="Descargar {file_label}"></a>'
    return href

@st.cache
def generate_list_of_stations():
    return pd.io.sql.read_sql('select distinct ACCC_TheMostFavouriteSite from DIM_Account',
                           server_connection(os.getcwd(), 'config_Comarch_diego'))

def generate_list_of_postal_code():
    return pd.io.sql.read_sql('select distinct CCD_PostalCode from DIM_ContactDetails',
                           server_connection(os.getcwd(), 'config_Comarch_diego'))

def validate_int_numbers(state, widget, type=None):
    if type:
        try:
            int(state.min_max_range[widget][type]['min'])
            int(state.min_max_range[widget][type]['max'])
        except:
            state.min_max_range[widget][type]['min'] = state.min_max_range_default[widget][type]['min']
            state.min_max_range[widget][type]['max'] = state.min_max_range_default[widget][type]['max']
            st.warning("Ingrese Número")
    else:
        try:
            int(state.min_max_range[widget]['min'])
            int(state.min_max_range[widget]['max'])
        except:
            state.min_max_range[widget]['min'] = state.min_max_range_default[widget]['min']
            state.min_max_range[widget]['max'] = state.min_max_range_default[widget]['max']
            st.warning("Ingrese Número")

if __name__ == "__main__":
    st_main()