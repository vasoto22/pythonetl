# import pandas as pd # pd alias de variable de pandas
# import matplotlib.pyplot as plt
# import seaborn as sns
# import numpy as np
# import plotly.express as px
# import plotly.graph_objects as go
# from extracion import Extracion as extra
# import os

# class Transformacion:
#     def __init__(self):
#         self.extraccion = extra()
#         self.datos_ruta={
#             'population' : 'data_population_world.csv',
#             'metadata' : 'metadata_countries.csv',
#             'codes' : 'country_list.csv',
#             'years_schooling' : 'mean-years-of-schooling-long-run.csv',
#             'countries_gdp' : 'countries_gdp_hist.csv',
#             'organizations_gdp' : 'organizations_gdp_hist.csv',
#         }
#         self.df = {}
#         self.carga_df()
    
#     def obtener_ruta(self):
#         ruta_actual = os.getcwd()
#         path_ds_world_population = os.path.join(ruta_actual,'src/etl/data/transformacion/').replace("\\","/")
#         return path_ds_world_population
    
#     def limpieza_columnas(self):
#         pass
    
#     def limpieza_nan(self, tipo=1):
#         pass
        
#     def carga_df(self, ruta=""):
#         for key, value in self.datos_ruta.items():
#             path_ds_world_population = os.path.join(self.obtener_ruta(),value)
#             if key == 'years_schooling' or key == 'metadata' or key == 'countries_gdp' or key == 'organizations_gdp':
#                  self.df[key] = self.extraccion.extracion_csv(path_ds_world_population, separador=';')
#             else:
#                 self.df[key] = self.extraccion.extracion_csv(path_ds_world_population, separador=',')
#             path_ds_world_population = os.path.join(self.obtener_ruta,value)
#             self.df[key] = self.extraccion.extracion_csv(path_ds_world_population, separador=',')
    
#     def unir_df(self, df1=object, df2=object):
#         pass


import pandas as pd # pd alias de variable de pandas
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from extracion import Extracion as extra
import os



class Transformacion:
    def __init__(self):
        self.extracion = extra()
        self.datos_ruta={
                'population ': 'data_population_world.csv', # key , value
                'metadata ': 'metadata_countries.csv',
                'codes ': 'country_list.csv',
                'years_schooling ': 'mean-years-of-schooling-long-run.csv',
                'countries_gdp ': 'countries_gdp_hist.csv',
                'organizations_gdp ': 'organizations_gdp_hist.csv'}
        self.df={}
        self.carga_df()
  
    def obtener_ruta(self):
        ruta_actual = os.getcwd()
        ruta_actual1 = (ruta_actual+'/src/etl/data/transformacion/').replace("\\","/")
        return  ruta_actual1
    
    
    def limpieza_columnas(self):
        pass
    
    def limpieza_nan(self,tipo=1):
        pass
    
    def carga_df(self):
        # df_avg_schooling = extracion.extracion_csv(path_years_schooling, separador=';')
        # df_countries_gdp = extracion.extracion_csv(path_countries_gdp, separador=';')
        for key, value in self.datos_ruta.items():
            
            ruta= os.path.join(self.obtener_ruta(),value)
            if key == 'years_schooling' or key == 'countries_gdp':
                print (key,"==",'countries_gdp')
                pass #self.df[key] = self.extracion.extracion_csv(ruta, separador=';')
            else:
                self.df[key] = self.extracion.extracion_csv(ruta, separador=',')

    
    def unir_df(self,df1=object,df2=object):
        pass
