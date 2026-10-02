from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NASDAQ").getOrCreate()

df = spark.read.csv(
    "data/nasdaq_etl_taller.csv",
    header=True,
    inferSchema=True
)

# Limpieza: eliminar duplicados y nulos
df = df.dropDuplicates()
df = df.dropna()

# Registrar DataFrame como tabla temporal
df.createOrReplaceTempView("nasdaq")

# Pregunta 1 ¿Cuántos registros contiene el dataset?
resultado = spark.sql("""
    SELECT COUNT(*) AS cantidad_registros
    FROM nasdaq
""")
resultado.show()
# Pregunta 2 ¿Cuál es el precio promedio de cierre (Close) considerando todas las acciones?
resultado = spark.sql("""
    SELECT AVG(Close) AS promedio_close
    FROM nasdaq
""")
resultado.show()
# Pregunta 3 ¿Cuál fue el precio de cierre máximo registrado?
resultado = spark.sql("""
    SELECT MAX(Close) AS precio_cierre_maximo
    FROM nasdaq
""")
resultado.show()
# Pregunta 4 ¿Cuál empresa presentó el mayor volumen total negociado?
resultado = spark.sql("""
    SELECT Ticker, SUM(Volume) AS volumen_total
    FROM nasdaq
    GROUP BY Ticker
    ORDER BY volumen_total DESC """)
resultado.show()
# Pregunta 5 ¿Cuál fue el volumen total negociado considerando todas las empresas?
resultado = spark.sql("""
    SELECT SUM(Volume) AS volumen_total
    FROM nasdaq
""")
resultado.show()
#Pregunta 6 ¿Los registros corresponden a días en los que el precio de cierre
# fue superior al precio de apertura son 15?
resultado =spark.sql("""
    SELECT COUNT(*) AS cantidad
    FROM nasdaq
    WHERE Close > Open""")
#La consulta cuenta los registros donde el precio de cierre 
# (Close) es mayor que el precio de apertura (Open). 
# Al comparar los 30 registros del dataset, se obtienen 
# 21 días en los 
# que el precio cerró por encima del precio de apertura.
resultado.show()
#Pregunta 7 ¿Cuál empresa presentó el mayor precio promedio de cierre?
resultado = spark.sql("""
    SELECT Ticker, AVG(Close) AS promedio_close
    FROM nasdaq
    GROUP BY Ticker
    ORDER BY promedio_close DESC """)
resultado.show()
# Pregunta 8 ¿El mayor rango diario registrado? Recuerde: Rango = High – Low es 9.
resultado =spark.sql("""
    SELECT MAX(High - Low) AS mayor_rango
    FROM nasdaq""")
resultado.show()
# Pregunta 9 ¿Cuál empresa presentó el mayor promedio de variación 
# porcentual entre apertura y cierre?
resultado = spark.sql("""
    SELECT Ticker,AVG(((Close - Open) / Open) * 100) AS variacion_promedio
    FROM nasdaq
    GROUP BY Ticker
    ORDER BY variacion_promedio DESC
""")
resultado.show()
#Pregunta 10 ¿Cuál fue el registro con mayor volumen individual?
resultado = spark.sql("""
    SELECT Ticker, Date, Volume
    FROM nasdaq
    ORDER BY Volume DESC
    LIMIT 1 """)
resultado.show()
spark.stop()