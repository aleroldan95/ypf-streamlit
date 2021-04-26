import pandas as pd

from streamlit.server.server import Server
from streamlit.report_thread import get_report_ctx
from streamlit.hashing import _CodeHasher
import streamlit as st

from PIL import Image
import base64
import io

class SessionState:
    def __init__(self, session, hash_funcs):
        "Initialize SessionState instance."
        self.__dict__["_state"] = {
            "data": {},
            "hash": None,
            "hasher": _CodeHasher(hash_funcs),
            "is_rerun": False,
            "session": session,
        }

    def __call__(self, **kwargs):
        "Initialize state data once."
        for item, value in kwargs.items():
            if item not in self._state["data"]:
                self._state["data"][item] = value

    def __getitem__(self, item):
        "Return a saved state value, None if item is undefined."
        return self._state["data"].get(item, None)

    def __getattr__(self, item):
        "Return a saved state value, None if item is undefined."
        return self._state["data"].get(item, None)

    def __setitem__(self, item, value):
        "Set state value."
        self._state["data"][item] = value

    def __setattr__(self, item, value):
        "Set state value."
        self._state["data"][item] = value

    def clear(self):
        "Clear session state and request a rerun."
        self._state["data"].clear()
        self._state["session"].request_rerun()

    def sync(self):
        "Rerun the app with all state values up to date from the beginning to fix rollbacks."

        # Ensure to rerun only once to avoid infinite loops
        # caused by a constantly changing state value at each run.
        #
        # Example: state.value += 1
        if self._state["is_rerun"]:
            self._state["is_rerun"] = False

        elif self._state["hash"] is not None:
            if self._state["hash"] != self._state["hasher"].to_bytes(self._state["data"], None):
                self._state["is_rerun"] = True
                self._state["session"].request_rerun()

        self._state["hash"] = self._state["hasher"].to_bytes(self._state["data"], None)


def get_session():
    session_id = get_report_ctx().session_id
    session_info = Server.get_current()._get_session_info(session_id)

    if session_info is None:
        raise RuntimeError("Couldn't get your Streamlit Session object.")

    return session_info.session


def get_state(hash_funcs=None):
    session = get_session()

    if not hasattr(session, "_custom_session_state"):
        session._custom_session_state = SessionState(session, hash_funcs)

    return session._custom_session_state

class State:
    def __init__(self):

        self.min_max_range = {'points_range': {'min': '0', 'max': '150000'}}

        self.is_first_state = False
        self.original = pd.DataFrame({'hola': [1]})
        self.query = pd.DataFrame({'hola': [1]})

        self.is_m = False
        self.is_f = False
        self.age_range = (0, 100)
        self.is_region_filtered = False
        self.province = ['Todas las Regiones']
        self.is_employee = False
        self.is_not_employee = False

        contact_types = ['Contacto', 'Email', 'Teléfono', 'SMS']
        self.contact_type = {}
        for contact in contact_types:
            self.contact_type[contact] = {'permite': False, 'no_permite': False}

        self.is_infinia = False
        self.is_super = False
        self.is_diesel = False
        self.is_ultra = False
        self.is_gnc = False
        self.socio_range = 60

        self.points_range = (0, 150000)

        self.active = False
        self.not_active = False
        self.is_in_app = False
        self.is_not_in_app = False
        self.is_using_app = False
        self.is_not_using_app = False

        self.redim_period = 30
        self.redim_range = (0, 200000)

        self.socio_period = 30
        self.socio_type = {}
        types = ['Infinia', 'Infinia Diesel', 'Super', 'Ultra', 'GNC']
        for ty in types:
            aux = {'period': 30, 'permite': False, 'no_permite': False}
            self.socio_type[ty] = aux

        self.stations1 = []
        self.stations2 = []
        self.list_of_available_stations = None
        self.ant_serviclub = (0, 50)

        types_comb = ['Infinia', 'Super', 'Infinia Diesel', 'Ultra', 'GNC', 'Boxes']
        self.st_variables_comb = {}
        for comb in types_comb:
            aux = {'period': 30, 'amount': (0, 3000000), 'avg': (0, 1000000)}
            self.st_variables_comb[comb] = aux

        self.rec = (0, 2000)
        self.freq = (0, 2000)

        app_types = ['Transacción', 'Litros', 'Frecuencia']
        self.st_variables_app = {}
        self.st_variables_app['Transacción'] = {'period': 30, 'amount': (0, 700000), 'max': 700000, 'freq': 10000}
        self.st_variables_app['Litros'] = {'period': 30, 'amount': (0, 7000), 'max': 7000, 'freq': 100}
        self.st_variables_app['Frecuencia'] = {'period': 30, 'amount': (0, 200), 'max': 200, 'freq': 10}

        types_payments = ['QR', 'App', 'Otros']
        self.st_variables_payment = {}
        for comb in types_payments:
            aux = {'period': 30, 'amount': (0, 1000000)}
            self.st_variables_payment[comb] = aux


class Style:
    def __init__(self):
        self.style = """<style>
                 @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700;800&display=swap');
                 .main {
                    background: linear-gradient(180deg, #9B78FF 0%, rgba(180, 155, 249, 0.453125) 71.53%, rgba(255, 255, 255, 0) 93.43%), #81BAFC;     
                }
                src="https://fonts.googleapis.com/icon?family=Material+Icons"

                .sidebar .sidebar-content .block-container{
                    font-color: #FFFFFF;
                }
                .sidebar .sidebar-content {
                    padding-top: 40px;
                    left: 0px;
                    top: 0px;
                    background: #724AE2;
                    box-shadow: 4px 4px 14px rgba(0, 0, 0, 0.25);
                    border-radius: 0px 8px 8px 0px;  
                    font-family: Montserrat;
                    font-style: normal;
                    font-weight: 600;
                    font-size: 12px;
                    line-height: 13px;
                    text-align: center;
                }
                .reportview-container .main .block-container{
                        position: absolute;
                        top: 15px;
                        bottom: 0px;
                        padding-top: 0px;
                        padding-left: 45px;
                        padding-right: 45px;
                        padding-bottom: 5px;
                        max-width: 80%;
                        box-shadow: 4px 4px 14px rgba(0, 0, 0, 0.25);
                        background: #FFFFFF;
                        box-sizing: border-box;
                        border: 1px;
                        text-size-adjust: 100%;                    
                } 
                 .Ag_2 {                     
                    font-family: Montserrat;
                    font-style: normal;
                    font-weight: bold;
                    font-size: 18px;
                    line-height: 22px;                          
                    color: #4D4D4D;
                    padding-top: 5px;
                    padding-bottom: 5px;
                 }
                 .Ag_sidebar_title{
                    font-family: Montserrat;
                    font-style: center;
                    font-weight: bold;
                    font-size: 16px;
                    line-height: 22px;
                    color: #FFFFFF;
                 }   
                .Ag_sidebar {
                    font-family: Montserrat;
                    text-align: center;
                    font-style: normal;
                    font-weight: bold;
                    font-size: 13px;
                    line-height: 22px;
                    color: #FFFFFF;    
                }
                 .Ag_1 {
                    font-family: Montserrat;
                    font-style: normal;
                    font-weight: 800;
                    font-size: 24px;
                    line-height: 29px;
                    color: #724AE2;
                    padding-top: 15px;
                    padding-bottom: 15px;
                }  
                .Ag {
                    font-family: Montserrat;
                    font-style: normal;
                    font-weight: bold;
                    font-size: 30px;
                    line-height: 37px;
                    padding-top: 15px;
                    padding-bottom: 15px;
                    color: #1673E1;
                }    
                .Ag_3 {                 
                    font-family: Montserrat;
                    font-style: normal;
                    font-weight: normal;
                    font-size: 14px;
                    line-height: 17px;
                    color: #000000;
                }
                </style>
                """

    def set_style(self):
        st.markdown(self.style, unsafe_allow_html=True)

    def ypf_side_bar(self):
        image = Image.open(r'Imagenes/ypf-logo-1.png')
        buffer = io.BytesIO()
        image.save(buffer, format='PNG')
        buffer.seek(0)
        data_uri = base64.b64encode(buffer.read()).decode('ascii')
        st.sidebar.markdown(f"""
                            <div id="image" style="padding-bottom: 10px;">
                                <img src='data:image/png;base64,{data_uri}' align="center"/>
                            </div>""", unsafe_allow_html=True)

    def ag(self, name):
        image = Image.open(r'Imagenes/ypf-logo-gris.png')
        buffer = io.BytesIO()
        image.save(buffer, format='PNG')
        buffer.seek(0)
        data_uri = base64.b64encode(buffer.read()).decode('ascii')
        st.markdown(f"""<div class="Ag">{name} 
                            <div id="image" style="display:inline;">
                                <img src='data:image/png;base64,{data_uri}' align="right"/>
                            </div>
                        </div>""", unsafe_allow_html=True)

    def ag1(self, name, icon_name):
        st.markdown(f"""<div class="Ag_1"><i class="material-icons">{icon_name}</i> {name}</div>""", unsafe_allow_html=True)

    def ag2(self, name, col=None):
        if col:
            col.markdown(f"""<div class="Ag_2">{name}</div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="Ag_2">{name}</div>""", unsafe_allow_html=True)

    def ag3(self, name, col=None):
        if col:
            col.markdown(f"""<div class="Ag_3">{name}</div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="Ag_3">{name}</div>""", unsafe_allow_html=True)

    def ag_sidebar_title(self, name):
        st.sidebar.markdown(f"""<div class="Ag_sidebar_title">{name}</div>""", unsafe_allow_html=True)

    def ag_siderbar(self, name):
        st.sidebar.markdown(f"""<div class="Ag_sidebar">{name}</div>""", unsafe_allow_html=True)

