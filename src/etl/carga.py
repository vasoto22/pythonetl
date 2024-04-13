# import sqlite3
# import pandas as pd

# class Carga: 
#     def __init__(self):
#         self.name_db = 'world_population.db'
    
#     def connect_db(self):
#         self.conn = sqlite3.connect(self.name_db)
#         self.cursor = self.conn.cursor()
        
#     def close_db(self):
#         self.conn.close()
    
#     def pandas_type_to_sqlite_type(self, dtype):
#         if pd.api.types.is_integer_dtype(dtype):
#             return 'INTEGER'
#         elif pd.api.types.is_float_dtype(dtype) or dtype == 'float64':
#             return 'REAL'
#         elif pd.api.types.is_datetime64_any_dtype(dtype):
#             return 'DATETIME'
#         else:
#             return 'TEXT'

#     def create_table(self, df=pd.DataFrame(), name_table=""):
#         columns = ", ".join([f"{col} {self.pandas_type_to_sqlite_type(dtype)}" for col, dtype in df.dtypes.iteritems()])
#         query = f'''
#         CREATE TABLE IF NOT EXISTS {name_table} (
#             {columns}
#         )
#         '''
#         self.cursor.execute(query)
#         self.conn.commit()
    
#     def insert_df_to_sql(self, name_table="", df=pd.DataFrame(), type_insert="append"):
#         df.to_sql(name_table, self.conn, if_exists="append")
import sqlite3
import pandas as pd
import os 
from extracion import Extracion as extra


class  Carga:
    def __init__(self,data_name="") :
        self.name_db=data_name
        
    def connect(self):
        self.conn = sqlite3.connect(self.name_db)
        self.cursor = self.conn.cursor()
        
    def close(self):
        self.conn.close()
        
    def create_table(self,df=pd.DataFrame(),name_tabla=""):
        query="CREATE TABLE IF NOT EXISTS prueba ({})"
        tipo_col=""
        n_col=1
        for col in df.columns:
            if n_col !=1:
                tipo_col = tipo_col + ", "
            if df[col].dtype == "object":
                tipo_col = tipo_col + " {} {} ".format(col,"String")
            elif df[col].dtype == "float64":
                tipo_col = tipo_col + " {} {} ".format(col,"float")
        print(query.format(tipo_col))
        
        self.cursor.execute(query.format(tipo_col))
        #print(query.format(tipo_col))
    
    def df_sql(self,name_table="",df=pd.DataFrame(),tipo ="append"):
        df.to_sql(name_table,self.conn,if_exists=tipo)
        

ruta_actual = os.getcwd()
print(ruta_actual)
extracion = extra()
path_ds_world_population = (ruta_actual+'/src/etl/df_world_data1.csv').replace("\\","/")
df_population = extracion.extracion_csv(path_ds_world_population, separador=';')

data_name = "mibasededatos.db"
carga = Carga(data_name)
carga.connect()
carga.create_table(name_tabla="data_population_world",df=df_population)
carga.df_sql(name_table="data_population_world",df=df_population)
carga.close()
