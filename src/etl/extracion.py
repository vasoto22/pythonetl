
# import pandas as pd
# import requests

# class Extracion:
#     def __init__(self) :
#         pass
    
#     def extracion_csv(self, ruta="",separador=""):
#         df = pd.read_csv(ruta,sep=separador)
#         if self.validar_df(df):
#             return df
        
#     def extracion_xlsx(self, ruta=""):
#         df = pd.read_excel(ruta)
#         if self.validar_df(df):
#             return df
    
    
#     def extracion_web(self, url=""):
#         response= requests.get(url)
#         df = pd.read_html(response.text)
#         if self.validar_df(df):
#             return df[0]
        
#     def validar_df(self,df=pd.DataFrame()):
#         if len(df)>0:
#             return True
#         return False

import pandas as pd
import requests

class Extracion:
    def __init__(self) :
        pass
    
    def extracion_csv(self, ruta="",separador=""):
        df = pd.read_csv(ruta,sep=separador)
        if self.validar_df(df):
            return df
        
    def extracion_xlsx(self, ruta=""):
        df = pd.read_excel(ruta)
        if self.validar_df(df):
            return df
    
    
    def extracion_web(self, url=""):
        response= requests.get(url)
        df = pd.read_html(response.text)
        if self.validar_df(df):
            return df[0]
        
    def validar_df(self,df=pd.DataFrame()):
        if len(df)>0:
            return True
        return False


   #'D:/ITM/2024_1/marzo_16_a_31/material_formacion/2024_04/data/transformacion/data_population_world.csv'
   #D:\ITM\2024_1\marzo_16_a_31\material_formacion\2024_04\src\etl\data\transformacion\data_population_world.csv
    



    
