import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import sys
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

def cargar_datos():
    try:
        # Configurar conexión SQLAlchemy para MySQL
        engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
        
        print("--- Iniciando Migración de Datos (Modo Seguro + Limpieza) ---")

        # 1. LIMPIEZA PREVIA - Resiliente a bases de datos nuevas o ya existentes
        print("Vaciando tablas para una carga limpia...")
        with engine.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            # DROP IF EXISTS permite que el script funcione tanto en bases de datos
            # recién creadas como en las que ya tienen datos
            conn.execute(text("DROP TABLE IF EXISTS analisis_eficiencia;"))
            conn.execute(text("DROP TABLE IF EXISTS precios;"))
            conn.execute(text("DROP TABLE IF EXISTS activos;"))
            # Recrear las tablas con la estructura correcta (incluyendo sharpe_ratio)
            conn.execute(text("""
                CREATE TABLE activos (
                    ticker_yahoo VARCHAR(15) PRIMARY KEY,
                    empresa VARCHAR(100) NOT NULL,
                    sector VARCHAR(50)
                );
            """))
            conn.execute(text("""
                CREATE TABLE precios (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    fecha DATE NOT NULL,
                    ticker_yahoo VARCHAR(15),
                    precio_cierre DECIMAL(12, 4),
                    precio_max DECIMAL(12, 4),
                    precio_min DECIMAL(12, 4),
                    volumen BIGINT,
                    FOREIGN KEY (ticker_yahoo) REFERENCES activos(ticker_yahoo) ON DELETE CASCADE
                );
            """))
            conn.execute(text("""
                CREATE TABLE analisis_eficiencia (
                    ticker_yahoo VARCHAR(15) PRIMARY KEY,
                    retorno_anualizado DECIMAL(10, 4),
                    riesgo_anualizado DECIMAL(10, 4),
                    burbuja_size DECIMAL(10, 4),
                    sharpe_ratio DECIMAL(8, 4),
                    FOREIGN KEY (ticker_yahoo) REFERENCES activos(ticker_yahoo) ON DELETE CASCADE
                );
            """))
            conn.execute(text("CREATE INDEX idx_fecha ON precios(fecha);"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
        print("✅ Base de datos lista para nueva carga.")

        # 2. Cargar Activos (Empresas + Benchmarks)
        print("Cargando tabla: activos...")
        df_comp = pd.read_csv('data/ibex35_componentes.csv')
        df_comp = df_comp[['ticker_yahoo', 'empresa', 'sector']]
        # Normalización de sectores para evitar duplicados por mayúsculas o espacios
        df_comp['sector'] = df_comp['sector'].str.strip().str.title()
        
        # AÑADIMOS BENCHMARKS (Para poder comparar en Tableau sin errores de FK)
        indices_mercado = pd.DataFrame([
            {'ticker_yahoo': '^IBEX', 'empresa': 'IBEX 35', 'sector': 'Índice de Mercado'},
            {'ticker_yahoo': '^VIX', 'empresa': 'VIX (Índice Volatilidad)', 'sector': 'Índice de Mercado'},
            {'ticker_yahoo': 'GC=F', 'empresa': 'Oro (Gold Futures)', 'sector': 'Materia Prima'}
        ])
        
        df_activos_final = pd.concat([df_comp, indices_mercado], ignore_index=True)
        df_activos_final.to_sql('activos', con=engine, if_exists='append', index=False)
        print("✅ Tabla 'activos' completada.")

        # 3. Cargar Precios Históricos
        print("Cargando tabla: precios...")
        df_long = pd.read_csv('data/ibex35_precios.csv')
        # La CSV ahora tiene Date, Ticker, precio_cierre, volumen
        df_long.rename(columns={'Date': 'fecha', 'Ticker': 'ticker_yahoo'}, inplace=True)
        try:
            df_long['fecha'] = pd.to_datetime(df_long['fecha']).dt.date
        except Exception:
            pass # Si ya era fecha
        
        # Filtramos para que entren los 35 componentes + los Benchmarks que acabamos de añadir
        lista_validos = df_activos_final['ticker_yahoo'].tolist()
        df_long = df_long[df_long['ticker_yahoo'].isin(lista_validos)]
        
        df_long.to_sql('precios', con=engine, if_exists='append', index=False)
        print("✅ Tabla 'precios' completada.")

        # Calcular y Cargar Análisis de Eficiencia
        print("Cargando tabla: analisis_eficiencia...")
        # Recreamos la tabla "ancha" (wide) para el cálculo de retornos
        df_p_long = pd.read_csv('data/ibex35_precios.csv')
        df_p = df_p_long.pivot(index='Date', columns='Ticker', values='precio_cierre')
        df_p.index = pd.to_datetime(df_p.index)
        
        df_rets = df_p.pct_change().dropna()
        retorno_total = (df_p.iloc[-1] / df_p.iloc[0] - 1)
        
        retorno_anual = df_rets.mean() * 252 * 100
        vol_anual = df_rets.std() * np.sqrt(252) * 100
        
        df_eff = pd.DataFrame({
            'ticker_yahoo': retorno_anual.index,
            'retorno_anualizado': retorno_anual.values,
            'riesgo_anualizado': vol_anual.values,
            'burbuja_size': retorno_total[retorno_anual.index].fillna(0).abs().values * 20,
            'sharpe_ratio': (retorno_anual / vol_anual).values
        })
        
        valid_tickers = df_comp['ticker_yahoo'].tolist()
        df_eff = df_eff[df_eff['ticker_yahoo'].isin(valid_tickers)]
        
        df_eff.to_sql('analisis_eficiencia', con=engine, if_exists='append', index=False)
        print("✅ Tabla 'analisis_eficiencia' completada.")

        print("Creando vistas para Tableau")
        with engine.begin() as conn:
            # Vista 1: Precios + Activos + Volumen
            conn.execute(text("""
                CREATE OR REPLACE VIEW view_precios_activos AS
                SELECT p.fecha, p.ticker_yahoo, a.empresa, a.sector, p.precio_cierre, p.precio_max, p.precio_min, p.volumen
                FROM precios p
                JOIN activos a ON p.ticker_yahoo = a.ticker_yahoo;
            """))
            # Vista 2: Eficiencia + Activos
            conn.execute(text("""
                CREATE OR REPLACE VIEW view_ranking_eficiencia AS
                SELECT e.ticker_yahoo, a.empresa, a.sector, e.retorno_anualizado, e.riesgo_anualizado, e.burbuja_size, e.sharpe_ratio
                FROM analisis_eficiencia e
                JOIN activos a ON e.ticker_yahoo = a.ticker_yahoo;
            """))
            # Vista 3: Variación Diaria (Para el Top Ganadores/Perdedores)
            conn.execute(text("""
                CREATE OR REPLACE VIEW view_variacion_diaria AS
                WITH UltimosPrecios AS (
                    SELECT 
                        p.fecha, 
                        p.ticker_yahoo, 
                        a.empresa,
                        a.sector,
                        p.precio_cierre,
                        p.precio_max,
                        p.precio_min,
                        LAG(p.precio_cierre) OVER(PARTITION BY p.ticker_yahoo ORDER BY p.fecha) as precio_anterior,
                        p.volumen
                    FROM precios p
                    JOIN activos a ON p.ticker_yahoo = a.ticker_yahoo
                )
                SELECT 
                    fecha, ticker_yahoo, empresa, sector, precio_cierre, precio_anterior, precio_max, precio_min, volumen,
                    ((precio_cierre - precio_anterior) / precio_anterior) * 100 as var_pct_diaria
                FROM UltimosPrecios
                WHERE fecha = (SELECT MAX(fecha) FROM precios);
            """))
            # Vista 4: Análisis Técnico (Medias Móviles SMA 50 y 200)
            conn.execute(text("""
                CREATE OR REPLACE VIEW view_analisis_tecnico AS
                SELECT 
                    p.fecha,
                    p.ticker_yahoo,
                    a.empresa,
                    p.precio_cierre,
                    p.volumen,
                    AVG(p.precio_cierre) OVER(PARTITION BY p.ticker_yahoo ORDER BY p.fecha ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as sma_50,
                    AVG(p.precio_cierre) OVER(PARTITION BY p.ticker_yahoo ORDER BY p.fecha ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) as sma_200
                FROM precios p
                JOIN activos a ON p.ticker_yahoo = a.ticker_yahoo;
            """))
        print("✅ Vistas de Tableau listas.")

        print("\n🚀 ¡MIGRACIÓN ESCALADA Y AUTOMATIZADA CON ÉXITO!")
        
    except Exception as e:
        print(f"\n❌ Error durante la migración: {e}")
        if "Access denied" in str(e):
            print("💡 Consejo: Revisa que la contraseña en tu archivo .env sea correcta.")
        elif "Unknown database" in str(e):
            print(f"💡 Consejo: Asegúrate de que la base de datos '{DB_NAME}' exista en MySQL.")

if __name__ == "__main__":
    if not DB_PASS or DB_PASS == "tu_password_aqui":
        print("⚠️  ALERTA: Debes configurar tus credenciales en el archivo '.env' antes de ejecutar.")
    else:
        cargar_datos()
