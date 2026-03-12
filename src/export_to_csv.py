import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Cargar credenciales
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

# Crear carpeta de exportación si no existe
os.makedirs("Datos/Tableau", exist_ok=True)

print("--- Exportando Vistas SQL a CSV para Tableau Public ---\n")

vistas = {
    "view_precios_activos":    "Datos/Tableau/precios_activos.csv",
    "view_ranking_eficiencia": "Datos/Tableau/ranking_eficiencia.csv",
    "view_variacion_diaria":   "Datos/Tableau/variacion_diaria.csv",
    "view_analisis_tecnico":   "Datos/Tableau/analisis_tecnico.csv"
}

for vista, ruta_csv in vistas.items():
    print(f"Exportando {vista}...")
    df = pd.read_sql(f"SELECT * FROM {vista}", con=engine)
    if vista == "view_precios_activos":
        df = df.sort_values(by=['empresa', 'fecha'])
        df['Retorno Diario'] = df.groupby('empresa')['precio_cierre'].pct_change()
    df.to_csv(ruta_csv, index=False, encoding='utf-8-sig')
    print(f"✅ {ruta_csv} ({len(df):,} filas guardadas)\n")

print("--- Calculando Matriz de Correlación para Tableau ---")

query_retornos = """
    SELECT empresa, fecha, precio_cierre 
    FROM view_precios_activos
"""
query_activos = """
    SELECT empresa, sector 
    FROM activos
    WHERE sector NOT IN ('Índice de Mercado', 'Materia Prima')
"""

df_precios_bd = pd.read_sql(query_retornos, con=engine)
df_sectores = pd.read_sql(query_activos, con=engine)

# Limpiamos los retornos históricos para que SOLO contengan empresas del catálogo limpio (sin benchmarks)
df_precios_bd = df_precios_bd[df_precios_bd['empresa'].isin(df_sectores['empresa'])]

df_pivote = df_precios_bd.pivot(index='fecha', columns='empresa', values='precio_cierre')
df_retornos_bd = df_pivote.pct_change().dropna()
matriz_corr = df_retornos_bd.corr()

matriz_corr = matriz_corr.reset_index()
matriz_tableau = matriz_corr.melt(id_vars='empresa', var_name='Activo_2', value_name='Correlacion')
matriz_tableau.rename(columns={'empresa': 'Activo_1'}, inplace=True)

matriz_tableau = matriz_tableau[matriz_tableau['Activo_1'] != matriz_tableau['Activo_2']]

# Cruzar ambos activos con su sector para Tableau
matriz_tableau = pd.merge(matriz_tableau, df_sectores, left_on='Activo_1', right_on='empresa', how='left')
matriz_tableau.rename(columns={'sector': 'Sector_Activo_1'}, inplace=True)
matriz_tableau = matriz_tableau.drop(columns=['empresa'])

matriz_tableau = pd.merge(matriz_tableau, df_sectores, left_on='Activo_2', right_on='empresa', how='left')
matriz_tableau.rename(columns={'sector': 'Sector_Activo_2'}, inplace=True)
matriz_tableau = matriz_tableau.drop(columns=['empresa'])

ruta_corr = "Datos/Tableau/matriz_correlacion.csv"
matriz_tableau.to_csv(ruta_corr, index=False, encoding='utf-8-sig')
print(f"✅ {ruta_corr} ({len(matriz_tableau):,} filas guardadas)\n")

print("--- Calculando Drawdown Histórico para Tableau ---")
# 1. Ya tenemos df_precios_bd filtrado sin índices ni materias primas
df_drawdown = df_precios_bd.copy()

# 2. Ordenamos cronológicamente para que la fórmula cummax() funcione perfecto
df_drawdown = df_drawdown.sort_values(by=['empresa', 'fecha'])

# 3. Calculamos el Precio Máximo Histórico alcanzado HASTA la fecha actual (por empresa)
df_drawdown['maximo_historico'] = df_drawdown.groupby('empresa')['precio_cierre'].cummax()

# 4. Calculamos el Drawdown: (Precio Actual - Máximo Histórico) / Máximo Histórico
df_drawdown['drawdown_pct'] = (df_drawdown['precio_cierre'] - df_drawdown['maximo_historico']) / df_drawdown['maximo_historico']

# 5. Cruzamos de nuevo con df_sectores para que Tableau tenga la dimensión Sector
df_drawdown = pd.merge(df_drawdown, df_sectores, on='empresa', how='left')

# 6. Guardamos
ruta_drawdown = "Datos/Tableau/drawdown_historico.csv"
df_drawdown.to_csv(ruta_drawdown, index=False, encoding='utf-8-sig')
print(f"✅ {ruta_drawdown} ({len(df_drawdown):,} filas guardadas)\n")


print(" ¡Exportación completada! CSVs listos en la carpeta Datos/Tableau/")

