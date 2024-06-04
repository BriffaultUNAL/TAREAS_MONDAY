import time
import logging
import sys
import os
import yaml
import sqlalchemy as sa
from sqlalchemy import text, Engine, Connection, Table, VARCHAR
from pandas.io.sql import SQLTable
from urllib.parse import quote
import pandas as pd
from pandas import DataFrame
from src.telegram_bot import enviar_mensaje
import subprocess
import re
import asyncio
import zipfile
from datetime import datetime

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

path_to_edgedriver = os.path.join(
    proyect_dir, 'edgedriver', 'msedgedriver.exe')
log_dir = os.path.join(proyect_dir, 'log', 'logs_main.log')
yml_credentials_dir = os.path.join(proyect_dir, 'config', 'credentials.yml')

file_name = os.listdir(os.path.join(act_dir, '..', 'data'))[0]

logging.basicConfig(
    level=logging.INFO,
    filename=(log_dir),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)


with open(yml_credentials_dir, 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1 = config['source1']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)


class Engine_sql:

    def __init__(self, username: str, password: str, host: str, database: str, port: str = 3306) -> None:
        self.user = username
        self.passw = password
        self.host = host
        self.dat = database
        self.port = port

    def get_engine(self) -> Engine:
        return sa.create_engine(f"mysql+pymysql://{self.user}:{quote(self.passw)}@{self.host}:{self.port}/{self.dat}?autocommit=true")

    def get_connect(self) -> Connection:
        return self.get_engine().connect()


engine_61 = Engine_sql(**source1).get_connect()


def to_sql_replace(table: SQLTable, con: Engine | Connection, keys: list[str], data_iter):

    satable: Table = table.table
    ckeys = list(map(lambda s: s.replace(' ', '_'), keys))
    data = [dict(zip(ckeys, row)) for row in data_iter]
    values = ', '.join(f':{nm}' for nm in ckeys)
    stmt = f"REPLACE INTO {satable.name} VALUES ({values})"
    con.execute(text(stmt), data)


def extraer_codigo(texto):
    patron = r'EAAA\d+'
    resultado = re.search(patron, texto)
    if resultado:
        return resultado.group()
    else:
        return None


def extraer_numero_documento(texto):
    patron = r'\d+$'
    resultado = re.search(patron, texto)
    if resultado:
        return resultado.group()
    else:
        return None


def eliminar_numero_documento(texto):
    return re.sub(r'\d+', '', texto)


def extract(file_path: str):
    df = pd.read_excel(file_path, header=2)
    return df


def transform(df: DataFrame):

    df.columns = df.columns.str.replace(' ', '_')

    df = df.dropna(how='all')

    df.fillna('', inplace=True)

    df = df[['Numero_de_Documento', 'Descripción_de_la_tarea',
            'Selecciona_el_área_al_que_perteneces', 'Estado_de_la_labor',
             'Prioridad_de_la_tarea', 'Fecha_inicio_de_la_labor', 'Fecha_de_finalización_de_la_labor',
             'Tarea_tipo', 'Labor', 'Selección_única', 'País_de_gestión']]

    df = df.rename(columns={
        'Numero_de_Documento': 'Documento',
        'Descripción_de_la_tarea': 'Descripcion_tarea',
        'Selecciona_el_área_al_que_perteneces': 'Area',
        'Prioridad_de_la_tarea': 'Prioridad',
        'Fecha_inicio_de_la_labor': 'Fecha_inicio',
        'Fecha_de_finalización_de_la_labor': 'Fecha_de_finalizacion',
        'Tarea_tipo': 'Tipo_de_tarea',
        'Labor': 'Labor_realizada',
        'Selección_única': 'Site',
        'País_de_gestión': 'Pais'})

    df['Fecha_Cargue'] = datetime.today()

    print(df.columns)

    return df


def transform_colombia(df: DataFrame):

    df.columns = df.columns.str.replace(' ', '_')

    df = df.dropna(how='all')

    df.fillna('', inplace=True)

    nombres = ['Almacén_-_Selecciona_tu_nombre',
               'Servicios_Generales_-_Selecciona_tu_nombre',
               'Seguridad_Física_-_Selecciona_tu_nombre',
               'Mantenimiento_-_Selecciona_tu_nombre']

    labor = ['Labor_realizada_-_Almacén', 'Labor_realizada_-_Servicios_Generales',
             'Labor_realizada_-_Seguridad_física', 'Labor_realizada_Mantenimiento']

    for col in nombres:
        df[col] = df[col].astype(str)

    for col in labor:
        df[col] = df[col].astype(str)

    df['Nombre'] = df.apply(lambda row: ''.join(row[nombres]), axis=1)

    df['Documento'] = df['Nombre'].apply(extraer_numero_documento)

    df['Nombre'] = df['Nombre'].apply(eliminar_numero_documento)

    df['Labor_realizada'] = df.apply(lambda row: ''.join(row[labor]), axis=1)

    print(df['Nombre'])
    print(df['Documento'])
    print(df['Labor_realizada'])

    df['Estado_de_la_labor'] = ''
    df['Prioridad'] = ''
    df['Tipo_de_tarea'] = ''
    df['Pais'] = 'Colombia'

    df = df[['Documento', 'Observaciones_de_la_labor_realizada',
            'Selecciona_el_área_al_que_perteneces', 'Estado_de_la_labor',
             'Prioridad', 'Fecha_y_hora_de_inicio_de_la_labor', 'Fecha_y_hora_de_finalización_de_la_labor',
             'Tipo_de_tarea', 'Labor_realizada', 'Seleccione_el_Site_de_Colombia', 'Pais']]

    df = df.rename(columns={
        'Observaciones_de_la_labor_realizada': 'Descripcion_tarea',
        'Selecciona_el_área_al_que_perteneces': 'Area',
        'Fecha_y_hora_de_inicio_de_la_labor': 'Fecha_inicio',
        'Fecha_y_hora_de_finalización_de_la_labor': 'Fecha_de_finalizacion',
        'Seleccione_el_Site_de_Colombia': 'Site'})

    df['Fecha_Cargue'] = datetime.today()

    print(df.columns)

    return df


def transform_mexico(df: DataFrame):

    df.columns = df.columns.str.replace(' ', '_')

    df = df.dropna(how='all')

    df.fillna('', inplace=True)

    nombres = ['Mantenimiento_-_Selecciona_tu_nombre',
               'Intendencia_-_Selecciona_tu_nombre',
               'Paquetería_-_Selecciona_tu_nombre']

    labor = ['Labor_realizada_-_Intendencia', 'Labor_realizada_-_Paquetería',
             'Labor_realizada_Mantenimiento']

    for col in nombres:
        df[col] = df[col].astype(str)

    for col in labor:
        df[col] = df[col].astype(str)

    df['Nombre'] = df.apply(lambda row: ''.join(row[nombres]), axis=1)

    df['Documento'] = df['Nombre'].apply(extraer_codigo)

    df['Labor_realizada'] = df.apply(lambda row: ''.join(row[labor]), axis=1)

    print(df['Documento'])
    print(df['Labor_realizada'])

    df['Estado_de_la_labor'] = ''
    df['Prioridad'] = ''
    df['Tipo_de_tarea'] = ''
    df['Pais'] = 'Mexico'

    df = df[['Documento', 'Observaciones_de_la_labor_realizada',
            'Selecciona_el_área_al_que_perteneces', 'Estado_de_la_labor',
             'Prioridad', 'Fecha_y_hora_de_inicio_de_la_labor', 'Fecha_y_hora_de_finalización_de_la_labor',
             'Tipo_de_tarea', 'Labor_realizada', 'Seleccione_el_Site_de_México', 'Pais']]

    df = df.rename(columns={
        'Observaciones_de_la_labor_realizada': 'Descripcion_tarea',
        'Selecciona_el_área_al_que_perteneces': 'Area',
        'Fecha_y_hora_de_inicio_de_la_labor': 'Fecha_inicio',
        'Fecha_y_hora_de_finalización_de_la_labor': 'Fecha_de_finalizacion',
        'Seleccione_el_Site_de_Colombia': 'Site'})

    df['Fecha_Cargue'] = datetime.today()

    return df


def load(df: DataFrame):
    with engine_61 as conn:
        df.to_sql('tb_crudo_monday_luis_alejandro', con=conn,
                  if_exists='append', index=False, method=to_sql_replace)
        logging.info(f'{len(df)} datos cargados')
