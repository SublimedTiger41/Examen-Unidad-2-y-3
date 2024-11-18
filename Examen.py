#Baca Lopez Jose Joaquin
#Nolasco Alejandro
from pprint import pformat

from sqlalchemy import create_engine
import pandas as pd

from Projects.transformacion_datos import crear_DF


def insercion_datos(ruta_csv, user, password, server, database):
    cadena_conexion = f"mysql+mysqlconnector://{user}:{password}@{server}/{database}"
    engine = create_engine(cadena_conexion)
    conexion = engine.connect()

    try:
        df = pd.read_csv(ruta_csv, delimiter=';')
        df = df[['Marca', 'Modelo', 'Precio']].copy()
        df['Cantidad'] = 1
        df.to_sql('ventas', conexion, if_exists='append', index=False)
        print("Se insertaron correctamente los datos")
    except Exception as e:
        print(f"Se produjo un error al insertar los datos {e}")
    finally:
        conexion.close()

def autos_vendidos_marca(user, password, server, database, marca=None):
    cadena_conexion = f"mysql+mysqlconnector://{user}:{password}@{server}/{database}"
    engine = create_engine(cadena_conexion)
    conexion = engine.connect()

    try:
        query = "SELECT Marca, Modelo, Precio, Cantidad FROM Ventas"
        df = pd.read_sql(query, conexion)
        if marca:
            df = df[df["Marca"] == marca]
        res = df.groupby("Marca").aggregate({'Cantidad': 'sum', 'Precio': 'sum'}).reset_index()
        return res
    except Exception as e:
        print(f"Se produjo un error al consultar los carros {e}")
    finally:
        conexion.close()

def carros_caros(user, password, server, database, n):
    if n <= 0:
        print("Tantita cabeza papito no pueden ser 0 carros")
        return None
    cadena_conexion = f"mysql+mysqlconnector://{user}:{password}@{server}/{database}"
    engine = create_engine(cadena_conexion)
    conexion = engine.connect()
    try:
        query = "SELECT Marca, Modelo, Precio FROM Ventas"
        df = pd.read_sql(query, conexion)

        if n > len(df):
            print("Se solicito la informacion de mas carros de los que hay")
            n = len(df)
        res = df.sort_values(by='Precio', ascending=False).head(n)
        return res
    except Exception as e:
        print(f"No se pudo consultar a los carros mas caros {e}")
    finally:
        conexion.close()

def filtrar_por_edad(min_edad, max_edad):
    df = pd.read_csv('C:/Users/bacaj/PycharmProjects/pythonProject/Projects/Examen_unidad_2_3/bank-loans.csv')
    df_filtrado = df[(df['age'] >= min_edad) & (df['age'] <= max_edad)]
    return df_filtrado

def mostrar_frecuencias_oficios():
    df = pd.read_csv('C:/Users/bacaj/PycharmProjects/pythonProject/Projects/Examen_unidad_2_3/bank-loans.csv')
    frecuencias = df['job'].value_counts()
    frecuencias_ordenadas = frecuencias.sort_values(ascending=False)
    print(frecuencias_ordenadas)

def mostrar_edades_medias_por_estudios():
    df = pd.read_csv('C:/Users/bacaj/PycharmProjects/pythonProject/Projects/Examen_unidad_2_3/bank-loans.csv')
    edades_medias = df.groupby('education')['age'].mean().sort_values(ascending=False)
    print(edades_medias)

def calcular_porcentaje_desconocido():
    df = pd.read_csv('C:/Users/bacaj/PycharmProjects/pythonProject/Projects/Examen_unidad_2_3/bank-loans.csv')
    porcentaje_desconocido = (df.isin(['unknown']).sum() / len(df)) * 100
    porcentaje_desconocido = porcentaje_desconocido[porcentaje_desconocido > 0]
    porcentaje_desconocido_ordenado = porcentaje_desconocido.sort_values(ascending=False)
    print(porcentaje_desconocido_ordenado)

def crear_dataframe():
    datos = {
        "Departamento": ["Ventas", "Ventas", "HR", "HR", "IT", "IT"],
        "Id": [1001, 1002, 2001, 2002, 3001, 3002],
        "Nombre": ["Juan", "Ana", "Luis", "Maria", "Pedro", "Sofía"],
        "Edad": [30, 24, 29, 25, 32, 28],
        "Salario": [40000, 42000, 38000, 39000, 50000, 52000]
    }
    return pd.DataFrame(datos).set_index(["Departamento","Id"])

def estadisticas_de_departamento(df,departamento):
    return df.loc[departamento][["Edad","Salario"]].agg(["mean","std","min","max"])

def fecha(df,fecha):
    df["Fecha"] = pd.to_datetime(fecha)
    return df

def filtrar_fecha(df,inicio,fin):
    return df[df["Fecha"].between(inicio,fin)]

def unir_archivos_csv(archivo1, archivo2, archivo_salida):
    try:
        df1 = pd.read_csv(archivo1)
        df2 = pd.read_csv(archivo2)

        df_resultante = pd.concat([df1, df2], ignore_index=True)
        df_resultante.to_csv(archivo_salida, index=False)

        return df_resultante
    except FileNotFoundError:
        print("Se produjo un error, uno o ambos archivos no existen o revisa la ruta por que no se pudo")

def agregar_info(df, archivo_info_extra):
    try:
        df_extra = pd.read_csv(archivo_info_extra)
        df_resultante = pd.merge(df, df_extra, on="Id", how="left")

        df_resultante.to_csv("empleados_con_info_extra.csv", index=False)
        return df_resultante
    except FileNotFoundError:
        print("Se produjo un error, el archivo que se busca no se encuentra")


if __name__ == "__main__":
    ruta_csv = "C:/Users/bacaj/PycharmProjects/pythonProject/Projects/Examen_unidad_2_3/coches.csv"
    user = "root"
    password = "8888j"
    server = "localhost"
    database = "coches"
    insercion_datos(ruta_csv, user, password, server, database)
    print("/////////Ejecicio 1/////////")
    print("a)")
    resultado_marca = autos_vendidos_marca(user, password, server, database, marca="Toyota")
    print("Autos vendidos por marca:")
    print(resultado_marca)
    print("b)")
    resultado_caros = carros_caros(user, password, server, database, n=5)
    print("\nCarros más caros:")
    print(resultado_caros)
    print("/////////Ejecicio 2/////////")
    print("a)")
    edad_minima = 25
    edad_maxima = 35
    resultado = filtrar_por_edad(edad_minima, edad_maxima)
    print("Clientes filtrados por edad:")
    print(resultado)
    print("b)")
    print("\nFrecuencias de los oficios ordenados de mayor a menor:")
    mostrar_frecuencias_oficios()
    print("c)")
    print("\nEdades medias según el nivel de estudios:")
    mostrar_edades_medias_por_estudios()
    print("d)")
    print("\nPorcentaje de valores unknown en cada columna:")
    calcular_porcentaje_desconocido()
    print("/////////Ejecicio 3/////////")
    print("a)")
    df = crear_dataframe()
    print("Dataframe de la empresa:\n",df)
    print("b)")
    print("\nEstadisticas del departamento de RH:\n",estadisticas_de_departamento(df,"HR"))
    print("c)")
    fechas = ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05", "2025-01-08"]
    df = fecha(df,fechas)
    print("\nDataframe con fechas:\n",df)
    print("d)")
    print("\nFiltrar entre 2025-01-01 y 2025-01-08:\n", filtrar_fecha(df, "2025-01-01", "2025-01-08"))
    empleados_2021 = "C:/Users/bacaj/PycharmProjects/pythonProject/Projects/Examen_unidad_2_3/empleados_2021.csv"
    empleados_2022 = "C:/Users/bacaj/PycharmProjects/pythonProject/Projects/Examen_unidad_2_3/empleados_2022.csv"
    archivo_salida = "C:/Users/bacaj/PycharmProjects/pythonProject/Projects/Examen_unidad_2_3/info_extra.csv"
    print("e)")
    df_unido = unir_archivos_csv(empleados_2021, empleados_2022, archivo_salida)
    if df_unido is not None:
        print("DataFrame despues de la fusion:")
        print(df_unido)
    print("f)")
    archivo_info_extra = "info_extra.csv"
    if df_unido is not None:
        df_completo = agregar_info(df_unido, archivo_info_extra)
        if df_completo is not None:
            print("DataFrame despues de meterle la info extra:")
            print(df_completo)




