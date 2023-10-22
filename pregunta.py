"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #

    columns = ['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave', 'principales_palabras_clave']
   
    widths = [5, 7, 19, 100]

    df = pd.read_fwf('clusters_report.txt', widths=widths, names=columns, skiprows=4)
    
    df_summary = df.copy()
    
    df_summary["grupo"] = df_summary["cluster"].ffill().bfill().astype(int)
    df_summary = df_summary.groupby("grupo").agg({
        "cluster": "first",
        "cantidad_de_palabras_clave": "first",
        "porcentaje_de_palabras_clave": "first",
        "principales_palabras_clave": lambda x: " ".join(x)
    }).reset_index(drop=True)
    df_summary["principales_palabras_clave"] = df_summary["principales_palabras_clave"].str.split().apply(lambda x: " ".join(x))
    df_summary["principales_palabras_clave"] = df_summary["principales_palabras_clave"].str.replace(".", "", regex=False)
    df_summary["porcentaje_de_palabras_clave"] = df_summary["porcentaje_de_palabras_clave"].str.replace("%", "", regex=False)
    df_summary["porcentaje_de_palabras_clave"] = df_summary["porcentaje_de_palabras_clave"].str.replace(",", ".", regex=False)
    df_summary["porcentaje_de_palabras_clave"] = df_summary["porcentaje_de_palabras_clave"].astype(float)
    df_summary["cantidad_de_palabras_clave"] = df_summary["cantidad_de_palabras_clave"].astype(int)
    df_summary["cluster"] = df_summary["cluster"].astype(int)
    df_summary.columns = df_summary.columns.str.replace(' ', '_', regex=False)

    return df_summary

ingest_data()