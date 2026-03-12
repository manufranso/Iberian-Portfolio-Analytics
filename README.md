# Iberian Portfolio Simulator — Análisis y Simulación 📈

Este proyecto desarrolla un simulador de inversiones basado en los 35 valores del **IBEX 35**, permitiendo analizar el comportamiento histórico, comparar sectores y visualizar el binomio riesgo frente al retorno de cada activo.

El sistema demuestra un pipeline de datos completo: desde la extracción automática de datos financieros hasta la visualización interactiva en Tableau diseñada específicamente para educar a inversores principiantes.

---

## 🛠️ Stack Tecnológico
- **Python 3.x** → ETL, análisis estadístico y carga de datos.
- **yfinance** → Descarga de datos históricos de Yahoo Finance.
- **pandas / numpy** → Limpieza y transformación de datos.
- **MySQL** → Base de datos relacional (gestionada vía Workbench).
- **SQLAlchemy** → Conector Python ↔ MySQL.
- **Tableau** → Dashboard interactivo final enfocado en la toma de decisiones.

---

## ⚙️ Fases del Proyecto

### 1. ETL (Extracción, Transformación y Carga)
- **Web scraping** de Wikipedia para obtener los 35 tickers actuales del IBEX 35.
- Descarga de **5 años de datos históricos** con métricas de OHLCV y Benchmarks (^IBEX, ^VIX).
- Limpieza profunda, gestión de nulos y normalización de nombres de sectores industriales españoles.

### 2. Análisis Exploratorio (EDA) y Estadística
- Identificación de tendencias y creación de matrices de correlación sectorial.
- Aplicación de tests estadísticos (**Shapiro-Wilk**) sobre los retornos, confirmando la no-normalidad y justificando la necesidad de una estrategia de diversificación.

### 3. Base de Datos MySQL
- Diseño de esquema relacional y creación de **Vistas Optimizadas** para Tableau, incluyendo comparativas de rentabilidad, variaciones diarias y análisis técnico .

---

## 📊 Visualización Interactiva (Tableau)
El proyecto final se organiza en dashboards pedagógicos que traducen tecnicismos financieros a lenguaje comprensible:

1.  **Situación de Mercado**: Resumen de la sesión con KPIs estilo "Bróker" (Cierre anterior, rangos intradiarios).
2.  **Matriz de Correlación**: Herramienta visual para entender la dependencia entre sectores y la importancia de la diversificación.
3.  **Simulador de Inversión (Calculadora)**: Herramienta de lógica LOD (Level of Detail) que permite simular beneficios netos según fecha de compra e inversión inicial.
4.  **Evolución del Índice**: Gráfico de histórico de precios para entender la tendencia a largo plazo del capital.

---

## � Conclusiones
- **Pipeline Automatizado**: El sistema permite una actualización dinámica de datos con un solo clic.
- **Valor Pedagógico**: Ayuda al inversor novato a entender conceptos de diversificación y simular escenarios financieros.
- **Decisión Basada en Datos**: Transforma la especulación en análisis de calidad profesional.

 
