import pandas as pd
import os
import re
import sqlite3
from datetime import datetime

# ==========================================
# BASE DE DATOS PARA HISTORIAL
# ==========================================
DB_NAME = "historial_bases_cargadas.db"

def inicializar_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS historial (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT,
            tipo_carga TEXT,
            ruta TEXT,
            registros_encontrados INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def registrar_historial(tipo_carga, ruta, registros):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute('INSERT INTO historial (fecha, tipo_carga, ruta, registros_encontrados) VALUES (?, ?, ?, ?)', 
                   (fecha, tipo_carga, ruta, registros))
    conn.commit()
    conn.close()

def obtener_historial():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query('SELECT * FROM historial ORDER BY id DESC', conn)
    conn.close()
    return df

# ==========================================
# LÓGICA DE LIMPIEZA Y EXTRACCIÓN
# ==========================================
def separar_telefonos(texto_crudo):
    """Separa los teléfonos según los criterios exigidos por WoodTools."""
    if pd.isna(texto_crudo): return []
    texto = str(texto_crudo).strip()
    
    # Criterio: Separar por /, //, Cel:, cel:, tel:, comas o punto y coma. Ignora guiones.
    partes = re.split(r'//|/|(?i)cel:?|(?i)tel:?|;|,|\|', texto)
    
    telefonos_limpios = []
    for parte in partes:
        num_puro = ''.join(filter(str.isdigit, parte))
        if len(num_puro) >= 6:
            telefonos_limpios.append(num_puro)
            
    vistos = set()
    return [x for x in telefonos_limpios if not (x in vistos or vistos.add(x))]

def estandarizar_columnas(df):
    """Intenta adivinar las columnas del Excel crudo y las renombra a un estándar interno."""
    cols_str = [str(c).lower().strip() for c in df.columns]
    
    mapa = {}
    for original, minuscula in zip(df.columns, cols_str):
        if 'nombre' in minuscula or ('cliente' in minuscula and 'n' not in minuscula): mapa[original] = 'Nombre'
        elif 'nro' in minuscula or 'número de cli' in minuscula or 'codigo' in minuscula: mapa[original] = 'Numero_Cliente'
        elif 'zona' in minuscula or 'locali' in minuscula or 'ciudad' in minuscula: mapa[original] = 'Zona_Cruda'
        elif 'vend' in minuscula: mapa[original] = 'Vendedor'
        elif 'tel' in minuscula or 'cel' in minuscula or 'num' in minuscula: mapa[original] = 'Telefonos_Raw'

    return df.rename(columns=mapa)

def procesar_archivos(lista_rutas):
    """Lee una lista de rutas (Excels/CSV) y los unifica en un solo DataFrame temporal."""
    dataframes_temporales = []
    total_filas = 0
    
    for ruta in lista_rutas:
        try:
            if ruta.endswith('.csv'): df_temp = pd.read_csv(ruta, dtype=str)
            else: df_temp = pd.read_excel(ruta, dtype=str)
            
            df_temp = estandarizar_columnas(df_temp)
            
            # Aseguramos columnas base aunque vengan vacías
            for col in ['Nombre', 'Numero_Cliente', 'Zona_Cruda', 'Vendedor', 'Telefonos_Raw']:
                if col not in df_temp.columns: df_temp[col] = ""
                
            dataframes_temporales.append(df_temp)
            total_filas += len(df_temp)
        except Exception as e:
            print(f"Error leyendo {ruta}: {e}")
            
    if dataframes_temporales:
        return pd.concat(dataframes_temporales, ignore_index=True), total_filas
    return pd.DataFrame(), 0

def procesar_cruce(df_maestro):
    """Agrupa duplicados y extrae hasta 5 teléfonos por cliente."""
    df = df_maestro.fillna("").astype(str)
    df['Telefonos_Lista'] = df['Telefonos_Raw'].apply(separar_telefonos)
    
    # Agrupamos por Número de Cliente para fusionar sus datos
    df_agrupado = df.groupby('Numero_Cliente', as_index=False).agg({
        'Nombre': 'first',
        'Zona_Cruda': 'first',
        'Vendedor': 'first',
        'Telefonos_Lista': 'sum' # Concatena las listas
    })
    
    datos_procesados = []
    for _, row in df_agrupado.iterrows():
        if not row['Nombre'] and not row['Numero_Cliente']: continue
        
        tels_unicos = list(dict.fromkeys(row['Telefonos_Lista']))
        zona_formateada = row['Zona_Cruda'] if row['Zona_Cruda'] else "Desconocida"
        
        registro = {
            'Nombre': row['Nombre'],
            'Número de cliente': row['Numero_Cliente'],
            'Zona del cliente': zona_formateada,
            'Vendedor': row['Vendedor'],
            'Primer número': tels_unicos[0] if len(tels_unicos) > 0 else "",
            'Segundo número': tels_unicos[1] if len(tels_unicos) > 1 else "",
            'Tercer número': tels_unicos[2] if len(tels_unicos) > 2 else "",
            'Cuarto número': tels_unicos[3] if len(tels_unicos) > 3 else "",
            'Quinto número': tels_unicos[4] if len(tels_unicos) > 4 else ""
        }
        datos_procesados.append(registro)
        
    return pd.DataFrame(datos_procesados)

def guardar_excel(df_final, ruta_guardar):
    """Guarda el DataFrame final en un archivo Excel."""
    df_final.to_excel(ruta_guardar, index=False)