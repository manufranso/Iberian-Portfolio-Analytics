from scipy import stats
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def test_normalidad(data, nombre_activo):
    """
    Realiza un análisis completo de normalidad sobre una serie de retornos.
    """
    # Test de Shapiro-Wilk 
    stat, p_value = stats.shapiro(data)
    
    # Cálculo de Asimetría y Curtosis
    skew = stats.skew(data)
    kurtosis = stats.kurtosis(data)
    
    resultado = {
        'activo': nombre_activo,
        'p_value': p_value,
        'skewness': skew,
        'kurtosis': kurtosis,
        'es_normal': p_value >= 0.05
    }
    
    return resultado

def imprimir_reporte_normalidad(res):
    """
    Imprime el resultado del test de normalidad.
    """
    print(f"Análisis Estadístico para {res['activo']}")
    print(f"p-value (Shapiro): {res['p_value']:.4f}")
    print(f"Asimetría (Skewness): {res['skewness']:.4f}")
    print(f"Curtosis (Exceso): {res['kurtosis']:.4f}")
    
    if not res['es_normal']:
        print("❌ Resultado: No sigue una distribución normal (Rechazamos H0)")
    else:
        print("✅ Resultado: Sigue una distribución normal (No rechazamos H0)")
    print("\n")

def graficar_distribucion(data, res):
    """
    Crea un histograma comparativo con la curva normal teórica.
    """
    plt.figure(figsize=(12, 6))
    
    # Histograma de los retornos reales
    sns.histplot(data, kde=True, stat="density", color='teal', alpha=0.4, label='Retornos Reales')
    
    # Curva Normal Teórica para comparar
    mu, std = data.mean(), data.std()
    x = np.linspace(data.min(), data.max(), 100)
    p = stats.norm.pdf(x, mu, std)
    plt.plot(x, p, 'r', linewidth=2, label='Normal Teórica')
    
    # Anotaciones de Skewness y Kurtosis
    plt.text(0.05, 0.95, f"Skewness: {res['skewness']:.2f}\nKurtosis: {res['kurtosis']:.2f}", 
             transform=plt.gca().transAxes, verticalalignment='top', 
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.5))
    
    plt.title(f"Distribución de Retornos: {res['activo']} vs. Normal Teórica", fontsize=15)
    plt.xlabel('Rentabilidad Diaria (%)')
    plt.ylabel('Densidad de Probabilidad')
    plt.legend()
    plt.show()

def analizar_sector_normalidad(df_retornos, df_comp, sector):
    """
    Realiza un análisis agregado de normalidad para todas las empresas de un sector.
    """
    # Normalizar nombres de columnas (limpiar espacios en los encabezados)
    df_comp.columns = [col.strip() for col in df_comp.columns]
    
    # Limpiar espacios en blanco en las celdas (por si acaso)
    df_comp['sector'] = df_comp['sector'].str.strip()
    df_comp['ticker_yahoo'] = df_comp['ticker_yahoo'].str.strip()

    # Filtrar tickers del sector
    tickers_sector = df_comp[df_comp['sector'] == sector]['ticker_yahoo'].tolist()
    
    # Asegurarnos de que los tickers estén en nuestro df_retornos
    tickers_validos = [t for t in tickers_sector if t in df_retornos.columns]
    
    if not tickers_validos:
        print(f"Advertencia: No se encontraron tickers para el sector '{sector}' en el DataFrame de retornos.")
        return None, pd.DataFrame()

    resultados_sector = []
    
    # Identificar columna de nombre (puede ser 'empresa' o 'nombre')
    columnas = df_comp.columns.tolist()
    if 'empresa' in columnas:
        col_nombre = 'empresa'
    elif 'nombre' in columnas:
        col_nombre = 'nombre'
    else:
        col_nombre = 'ticker_yahoo'
    
    for ticker in tickers_validos:
        # Buscamos el nombre real para el reporte
        nombre_real = df_comp[df_comp['ticker_yahoo'] == ticker][col_nombre].iloc[0]
        res = test_normalidad(df_retornos[ticker], nombre_real)
        resultados_sector.append(res)
    
    # Crear un DataFrame con los resultados
    df_res = pd.DataFrame(resultados_sector)
    
    if df_res.empty:
        return None, df_res

    summary = {
        'sector': sector,
        'n_empresas': len(tickers_validos),
        'p_value_medio': df_res['p_value'].mean(),
        'kurtosis_max': df_res['kurtosis'].max(),
        'kurtosis_media': df_res['kurtosis'].mean(),
        'skewness_media': df_res['skewness'].mean(),
        'porcentaje_normal': (df_res['es_normal'].sum() / len(tickers_validos)) * 100
    }
    
    return summary, df_res

def graficar_comparativa_sectores(df_retornos, df_comp, lista_sectores):
    """
    Crea una comparativa visual de las distribuciones de varios sectores.
    """
    plt.figure(figsize=(14, 7))
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    # Limpieza previa del CSV
    df_comp_clean = df_comp.copy()
    df_comp_clean.columns = [col.strip() for col in df_comp_clean.columns]
    df_comp_clean['sector'] = df_comp_clean['sector'].str.strip()
    df_comp_clean['ticker_yahoo'] = df_comp_clean['ticker_yahoo'].str.strip()

    for i, sector in enumerate(lista_sectores):
        tickers = df_comp_clean[df_comp_clean['sector'] == sector]['ticker_yahoo'].tolist()
        tickers_validos = [t for t in tickers if t in df_retornos.columns]
        
        if tickers_validos:
            # Calculamos el retorno promedio del sector para visualizar su distribución
            retorno_sector = df_retornos[tickers_validos].mean(axis=1)
            sns.kdeplot(retorno_sector, fill=True, label=f"Sector: {sector}", color=colors[i % len(colors)], alpha=0.3)
    
    plt.title("Comparativa de Distribuciones por Sector", fontsize=15)
    plt.xlabel("Rentabilidad Diaria Promedio (%)")
    plt.ylabel("Densidad")
    plt.axvline(0, color='grey', linestyle='--', alpha=0.5)
    plt.legend()
    plt.show()

def graficar_matriz_correlacion(df_retornos, titulo="Matriz de Correlación de Retornos", df_comp=None):
    """
    Calcula y visualiza la matriz de correlación de Pearson.
    Si se proporciona df_comp, mapea los tickers a nombres de empresas.
    """
    import seaborn as sns
    import matplotlib.pyplot as plt
    
    corr_matrix = df_retornos.corr()
    
    # Mapeo de nombres si se proporciona el dataframe de componentes
    if df_comp is not None:
        df_comp_copy = df_comp.copy()
        # Normalizar nombres de columnas
        df_comp_copy.columns = [col.strip() for col in df_comp_copy.columns]
        df_comp_copy['ticker_yahoo'] = df_comp_copy['ticker_yahoo'].str.strip()
        
        # Identificar columna de nombre
        columnas = df_comp_copy.columns.tolist()
        if 'empresa' in columnas:
            col_nombre = 'empresa'
        elif 'nombre' in columnas:
            col_nombre = 'nombre'
        else:
            col_nombre = 'ticker_yahoo'
            
        df_comp_copy[col_nombre] = df_comp_copy[col_nombre].str.strip()
        
        mapeo = dict(zip(df_comp_copy['ticker_yahoo'], df_comp_copy[col_nombre]))
        
        # Renombramos el índice y las columnas para el gráfico
        nombres_legibles = [mapeo.get(ticker, ticker) for ticker in corr_matrix.columns]
        corr_data = corr_matrix.copy()
        corr_data.columns = nombres_legibles
        corr_data.index = nombres_legibles
    else:
        corr_data = corr_matrix

    plt.figure(figsize=(16, 12))
    sns.heatmap(corr_data, annot=False, cmap='coolwarm', center=0,
                linewidths=0.5, linecolor='white')
    
    plt.title(titulo, fontsize=16)
    plt.show()
    
    return corr_matrix
