
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


    
