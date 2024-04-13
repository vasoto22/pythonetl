import json
import os
import pandas as pd
from sqlalchemy import create_engine,inspect
from sqlalchemy.exc import NoSuchTableError
from datetime import datetime
import logging
import re

class Logs:
    def __init__(self):
        self.fecha_hora = datetime.now().strftime('%Y%m%d_%H%M%S')
        logging.basicConfig(filename=f'logs/log_{self.fecha_hora}.txt', level=logging.INFO,filemode='a',  # Añade 'filemode' con valor 'a' para asegurar el append
                            format='%(asctime)s - %(levelname)s - %(message)s')

    def log(self, mensaje, nivel):
        if nivel == 'info':
            logging.info(mensaje)
        elif nivel == 'error':
            logging.error(mensaje)

class Extraccion:
    def __init__(self):
        self.logs = Logs()

    def cargar_desde_archivo(self, ruta_archivo):
        try:
            self.logs.log(f'Intentar cargar la ruta {ruta_archivo}', 'info')
            if ruta_archivo.endswith('.csv'):
                self.logs.log(f'valida extension  .csv {ruta_archivo}', 'info')
                df = pd.read_csv(ruta_archivo,index_col=False)
            elif ruta_archivo.endswith('.xlsx'):
                self.logs.log(f'valida extension  .xlsx {ruta_archivo}', 'info')
                df = pd.read_excel(ruta_archivo)
                self.logs.log(f'valida extension  .txt {ruta_archivo}', 'info')
            elif ruta_archivo.endswith('.txt'):
                df = pd.read_csv(ruta_archivo, delimiter = "\t",index_col=False)
            return df
        except Exception as e:
            hora = datetime.now().strftime('%H%M%S')
            self.logs.log(f' hora : {hora} Error al cargar datos desde el archivo {ruta_archivo}: {str(e)}', 'error')

    def cargar_desde_carpeta(self, ruta_carpeta):
        try:
            archivos = [os.path.join(ruta_carpeta, archivo) for archivo in os.listdir(ruta_carpeta) if archivo.endswith(('.csv', '.xlsx', '.txt'))]
            dfs = [self.cargar_desde_archivo(archivo) for archivo in archivos]
            # df_final = pd.concat(dfs, ignore_index=True)
            return dfs
        except Exception as e:
            self.logs.log(f'Error al cargar datos desde la carpeta {ruta_carpeta}: {str(e)}', 'error')

from sqlalchemy.exc import NoSuchTableError

class Carga:
    def __init__(self, db_string):
        self.engine = create_engine(db_string)
        self.logs = Logs()

    def cargar_a_bd(self, df, tabla, accion='replace'):
        try:
            inspector = inspect(self.engine)
            if accion not in ['replace', 'append', 'update']:
                raise ValueError('La acción debe ser "replace", "append" o "update"')

            # Verificar si la tabla existe
            #if self.engine.dialect.has_table(self.engine, tabla):
            if inspector.has_table(tabla):
                if accion == 'replace':
                    df.to_sql(tabla, self.engine, if_exists='replace')
                elif accion == 'append':
                    df.to_sql(tabla, self.engine, if_exists='append')
                elif accion == 'update':
                    # Actualizar los datos requiere un proceso más complejo, aquí hay un ejemplo simple
                    df_existente = pd.read_sql_table(tabla, self.engine)
                    df_existente.update(df)
                    df_existente.to_sql(tabla, self.engine, if_exists='replace', index=False)
            else:
                # Si la tabla no existe, la creamos
                df.to_sql(tabla, self.engine, if_exists='replace', index=False)#, schema='etl')
        except Exception as e:
            self.logs.log(f'Error al cargar datos en la tabla {tabla}: {str(e)}', 'error')

class Transformacion:
    def __init__(self, db_string):
        self.extraccion = Extraccion()
        self.carga = Carga(db_string)
        self.logs = Logs()
        self.dfs=[]

    def jupyter_class(self,dfs=[]):
        self.dfs=dfs
        df_final = pd.DataFrame()
        self.dfs[0]=pd.melt(self.dfs[0],
                         id_vars=['Country Name','Country Code','Indicator Name','Indicator Code'],
                         value_vars=self.dfs[0].iloc[:,4:-1].columns,var_name='year',
                         value_name=('total'))
        self.dfs[0] = self.estandarizar_columnas(self.dfs[0])
        self.dfs[0].columns =['country_name', 'country_code', 'indicator_name', 'indicator_code',
       'year','population']
        self.dfs[0] = self.dfs[0].dropna(subset=['population'])
        self.dfs[0] =self.dfs[0][['country_name', 'country_code', 'indicator_name', 'indicator_code',
       'year','population']]
        self.dfs[1].columns =['country_name','country_code','region','income_group','n']
        self.dfs[1]['region'] =self.dfs[1]['region'].fillna('Sin region')
        df_final = self.dfs[0].merge(self.dfs[1],left_on = 'country_code',
                                           right_on = 'country_code',how='inner')
        df_final = df_final [['country_name_x','country_code','region','income_group','year','population']]
        df_final['rate_pop'] = df_final.groupby(['country_name_x'],group_keys=False)['population'].pct_change()*100
        df_final['pop_millon'] = df_final['population']/1000000
        df_final['pop_millon'] = df_final['pop_millon'].round(1)
        df_final['rate_pop_millon'] = df_final['rate_pop'].round(1)
        df_final = df_final.merge(self.dfs[2],left_on = 'country_code',
                                           right_on = 'alpha-3',how='inner')
        df_final = df_final[df_final['country_code'].isin(df_final['country_code'])]
        df_final = df_final[['country_name_x','country_code','region_y','sub-region','income_group','year','population','pop_millon','rate_pop','rate_pop_millon']]
        df_final.columns= ['country_code','country_name','region_name','sub_region_name','income_group','year','population','pop_millon','rate_pop','rate_pop_millon']

        return df_final
        

    def ejecutar_operaciones(self, df, operaciones):
        for operacion in operaciones:
            if operacion['tipo'] == 'estandarizar_columnas':
                df = self.estandarizar_columnas(df)
            elif operacion['tipo'] == 'reemplazar_nans':
                df = self.reemplazar_nans(df, operacion['valor'])
            elif operacion['tipo'] == 'filtrar_df':
                df = self.filtrar_df(df, operacion['filtro'])
            elif operacion['tipo'] == 'imprimir_info':
                self.logs.log(f'******** Informacion del Dataframe *******************************', 'info')
                self.logs.log(f'nombre columnas antes : {df.columns}', 'info')
                df = self.estandarizar_columnas(df)
                self.logs.log(f'nombre columnas despues : {df.columns}', 'info')
                self.imprimir_info(df)
            elif operacion['tipo'] == 'jupyter_class': 
                self.jupyter_class(df) 
        return df

    def validar_df(self, df):
        if df.empty:
            self.logs.log('El DataFrame está vacío', 'error')
            return False
        return True

    def estandarizar_columnas(self, df):
        df.columns = [re.sub(r'\W+', '', col.lower().replace(' ', '_')) for col in df.columns] #lamda
        return df

    def reemplazar_nans(self, df, valor):
        if isinstance(valor, str):
            df.fillna(valor, inplace=True)
        elif isinstance(valor, (int, float)):
            df.fillna(valor, inplace=True)
        return df

    def imprimir_info(self, df):
        self.logs.log(f'******** Informacion del Dataframe *******************************', 'info')
        self.logs.log(f'{df.info()}', 'info')
        self.logs.log(f'******** Informacion del descripcion del dataframe ***************', 'info')
        self.logs.log(f'{df.describe()}', 'info')
        self.logs.log(f'************ total de NANs del dataframe *************************', 'info')
        self.logs.log(f'{df.isna().sum()}', 'info')
        # print(df.info())
        # print(df.describe())
        # print(df.isna().sum())

    def cruzar_dfs(self, df1, df2, tipo_cruce, cols_cruce):
        if tipo_cruce == 'inner':
            df_final = pd.merge(df1, df2, on=cols_cruce)
        elif tipo_cruce == 'outer':
            df_final = pd.merge(df1, df2, on=cols_cruce, how='outer')
        elif tipo_cruce == 'left':
            df_final = pd.merge(df1, df2, on=cols_cruce, how='left')
        elif tipo_cruce == 'right':
            df_final = pd.merge(df1, df2, on=cols_cruce, how='right')
        return df_final

    def limpiar_columnas_repetidas(self, df):
        cols_repetidas = df.columns[df.columns.duplicated()]
        for col in cols_repetidas:
            if df[col].nunique() == 1:
                df = df.loc[:,~df.columns.duplicated()]
        return df

    def filtrar_df(self, df, filtro):
        for col, val in filtro.items():
            df = df[df[col] == val]
        return df

    def transformar_y_cargar(self, archivos, tabla):
        try:
            dfs = [self.extraccion.cargar_csv(archivo) for archivo in archivos]
            df_final = pd.concat(dfs, ignore_index=True)
            if self.validar_df(df_final):
                df_final = self.estandarizar_columnas(df_final)
                self.carga.cargar_a_bd(df_final, tabla)
        except Exception as e:
            self.logs.log(f'Error al transformar y cargar los datos: {str(e)}', 'error')


class Main:
    def __init__(self, db_string):
        self.transformacion = Transformacion(db_string)
        self.logs = Logs()

    def ejecutar(self, config_file):
        try:
            with open(config_file) as f:
                config = json.load(f)
            archivos = config['archivos']
            tabla = config['tabla']
            self.dfs=[]
            for archivo in archivos:
                df = self.transformacion.extraccion.cargar_desde_archivo(archivo)
                self.dfs.append(df)
            df_final =self.transformacion.jupyter_class(self.dfs)
            if df_final is not None:
                operaciones = config['operaciones']
                df_final = self.transformacion.ejecutar_operaciones(df_final, operaciones)
                if "unnamed_0" in df_final.columns:
                    df_final = df_final.drop(df_final.columns[0], axis=1)
                #df_final.to_excel('datos_mundo.xlsx', index=False)
                self.transformacion.carga.cargar_a_bd(df_final, tabla) 
        except Exception as e:
            self.logs.log(f'Error al ejecutar el proceso ETL: {str(e)}', 'error')

if __name__ == "__main__":
    db_string = 'postgresql://postgres:admin@localhost:5432/etl'  # Reemplaza con tu cadena de conexión
    config_file = 'C:/src/procesamientodeetl/dev/2024_04-main/src/etl/static/config.json'   # Reemplaza con la ruta a tu archivo de configuración
    main = Main(db_string)
    main.ejecutar(config_file)
