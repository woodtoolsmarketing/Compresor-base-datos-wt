import pandas as pd
import numpy as np
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
# INTELIGENCIA DE ZONAS 
# ==========================================
MAPA_ZONAS = {
    '101': 'ZONA NORTE (San Fernando, Tigre, V. Lopez, San Martin)',
    '102': 'ZONA SUR (Avellaneda, Lanus, Quilmes, Bernal)',
    '103': 'LA PLATA (Tolosa, Villa Elisa, City Bell, Centro)',
    '104': 'ZONA SUR (Lomas, Temperley, Monte Grande, Ezeiza)',
    '107': 'ZONA OESTE (Merlo, Moron, San Justo, Caseros, Ramos Mejia)',
    '110': 'ZONA CAPITAL (Pompeya, Paternal, Barracas)',
    '115': 'RUTA 29 (Gral. Belgrano, Brandsen, San Vicente)',
    '120': 'RUTA 2 (Chascomus, Dolores, Lezama, La Costa)',
    '122': 'ZONA SUR (Zapala, Junin de los Andes, Bariloche)',
    '124': 'ZONA 124 (Jauregui, Lujan)',
    '130': 'RUTA 5 (Trenque Lauquen, Rufino, Olavarria)',
    '132': 'BAHIA BLANCA / SUR (Olavarria, Tres Arroyos, Tandil)',
    '136': 'ENTRE RIOS (Gualeguaychu, Concordia, Parana)',
    '137': 'CORDOBA (Capital, Carlos Paz, Rio IV)',
    '140': 'ZONA 140 (Areco, Pergamino, San Nicolas)',
    '141': 'LUJAN / MERCEDES (Giles, Pilar, Mercedes)',
    '142': 'ROSARIO (Santa Fe, San Nicolas, Ramallo)',
    '143': 'ZONA 143 (Urdinarrain, Victoria, Crespo)',
    '144': 'RUTA 5 (Giles, Areco, Salto, Rojas)',
    '146': 'SALTA (Salta, Catamarca, Tucuman, Jujuy)',
    '148': 'RUTA 3 (Las Flores, Azul, Rauch)',
    '149': 'ESPERANZA (Esperanza, Rafaela, Crespo)'
}

def extraer_zona_inteligente(texto_fila, zona_cruda):
    """Busca a fondo la zona real combinando todas las celdas del cliente."""
    texto = (str(texto_fila) + " | " + str(zona_cruda)).upper()
    
    # 1. Búsqueda estricta del código numérico (ej: 103, 142)
    matches = re.findall(r'\b(1[0-4]\d)\b', texto)
    for m in matches:
        if m in MAPA_ZONAS:
            return f"{m} | {MAPA_ZONAS[m]}"
            
    # 2. Búsqueda por palabra clave descriptiva (ej: "ZONA OESTE")
    for num, desc in MAPA_ZONAS.items():
        clave = desc.split('(')[0].strip().upper()
        if clave in texto and clave not in ["ZONA", "RUTA", "ZONA SUR", "ZONA NORTE"]:
            return f"{num} | {desc}"
            
    # 3. Genéricos
    if "ZONA SUR" in texto: return "102 o 104 | ZONA SUR"
    if "ZONA NORTE" in texto: return "101 | ZONA NORTE"
    
    # 4. Si no encontró en la base, deja lo que había
    zc = str(zona_cruda).strip()
    if zc and zc.lower() != 'nan':
        return zc
        
    return "Desconocida"

# ==========================================
# LÓGICA DE LIMPIEZA Y EXTRACCIÓN
# ==========================================
def separar_telefonos(texto_crudo):
    if pd.isna(texto_crudo): return []
    texto = str(texto_crudo).strip()
    
    # Pre-limpieza para evitar que se coma los CUITs y las fechas
    texto = re.sub(r'\b\d{2}-\d{8}-\d{1}\b', '', texto)
    texto = re.sub(r'\b\d{2}/\d{2}/\d{4}\b', '', texto)
    
    partes = re.split(r'//|/|\*|_|cel:?|tel:?|móvil:?|movil:?|contacto:?|;|,|\|', texto, flags=re.IGNORECASE)
    
    telefonos_limpios = []
    for parte in partes:
        num_puro = ''.join(filter(str.isdigit, parte))
        if len(num_puro) >= 8 and len(num_puro) <= 15:
            telefonos_limpios.append(num_puro)
            
    vistos = set()
    return [x for x in telefonos_limpios if not (x in vistos or vistos.add(x))]

def estandarizar_columnas(df):
    cols_str = [str(c).lower().strip() for c in df.columns]
    mapa = {}
    asignados = set() # <--- ESCUDO: Recuerda qué columnas ya renombramos
    
    for original, minuscula in zip(df.columns, cols_str):
        nuevo_nombre = None
        if 'vend' in minuscula: 
            nuevo_nombre = 'Vendedor'
        elif 'zona' in minuscula or 'locali' in minuscula or 'ciudad' in minuscula or 'ubic' in minuscula: 
            nuevo_nombre = 'Zona_Cruda'
        elif 'tel' in minuscula or 'cel' in minuscula or 'móvil' in minuscula or 'contacto' in minuscula: 
            nuevo_nombre = 'Telefonos_Raw'
        elif 'cód' in minuscula or 'cod' in minuscula or 'nro' in minuscula or 'código' in minuscula: 
            nuevo_nombre = 'Numero_Cliente'
        elif 'nombre' in minuscula or 'cliente' in minuscula or 'razon' in minuscula or 'razón' in minuscula or 'social' in minuscula: 
            nuevo_nombre = 'Nombre'
            
        # Si encontró un nombre y NO lo usamos todavía, lo renombra.
        if nuevo_nombre and nuevo_nombre not in asignados:
            mapa[original] = nuevo_nombre
            asignados.add(nuevo_nombre)
            
    df = df.rename(columns=mapa)
    
    # MUY IMPORTANTE: Elimina cualquier columna que haya quedado duplicada y rompa Pandas
    df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

def primer_valido(series):
    """Extrae el primer dato real ignorando celdas vacías."""
    s = series.astype(str).str.strip().replace(['nan', 'None', ''], np.nan).dropna()
    return s.iloc[0] if not s.empty else ""

def procesar_un_archivo(ruta):
    try:
        if ruta.endswith('.csv'): df_temp = pd.read_csv(ruta, dtype=str, header=None)
        else: df_temp = pd.read_excel(ruta, dtype=str, header=None)
        
        # BUSCADOR INTELIGENTE DE ENCABEZADO
        header_idx = 0
        encabezado_encontrado = False
        for idx, row in df_temp.head(30).iterrows():
            if sum(row.fillna("").astype(str).str.strip() != "") >= 3:
                row_str = " ".join(row.fillna("").astype(str)).lower()
                if ("cód" in row_str or "cod" in row_str or "nro" in row_str) and \
                   ("nombre" in row_str or "razon" in row_str or "cliente" in row_str):
                    header_idx = idx
                    encabezado_encontrado = True
                    break
        
        if not encabezado_encontrado: header_idx = 0

        df_temp.columns = df_temp.iloc[header_idx].fillna("Col_Vacia").astype(str)
        df_temp = df_temp.iloc[header_idx+1:].reset_index(drop=True)
        df_temp = estandarizar_columnas(df_temp)
        
        # SÚPER ESCÁNER: Todo el texto a una sola celda
        df_temp['Row_String'] = df_temp.apply(lambda row: ' | '.join(row.dropna().astype(str)), axis=1)
        
        for col in ['Nombre', 'Numero_Cliente', 'Zona_Cruda', 'Vendedor']:
            if col not in df_temp.columns: df_temp[col] = ""
            
        # Rellenar Número de Cliente hacia abajo
        df_temp['Numero_Cliente'] = df_temp['Numero_Cliente'].replace(r'^\s*$', np.nan, regex=True).ffill()
        df_temp['Numero_Cliente'] = np.where(df_temp['Numero_Cliente'].isna(), "SinID_" + df_temp.index.astype(str), df_temp['Numero_Cliente'])
        
        # Vectorización Rápida
        for col in ['Nombre', 'Vendedor', 'Zona_Cruda']:
            df_temp[col] = df_temp[col].replace([r'^\s*$', 'nan', 'None'], np.nan, regex=True)
            df_temp[col] = df_temp.groupby('Numero_Cliente')[col].transform(lambda x: x.ffill().bfill())
            df_temp[col] = df_temp[col].fillna("")
            
        text_agg = df_temp.groupby('Numero_Cliente')['Row_String'].apply(lambda x: ' | '.join(x.astype(str))).reset_index()
        
        df_agrupado = df_temp.drop_duplicates(subset=['Numero_Cliente']).copy()
        df_agrupado = df_agrupado.drop(columns=['Row_String']).merge(text_agg, on='Numero_Cliente', how='left')
        
        return df_agrupado, len(df_temp)
    except Exception as e:
        print(f"Archivo omitido por error: {ruta} -> {e}")
        return pd.DataFrame(), 0

def procesar_cruce(df_maestro, progress_callback=None):
    try:
        if progress_callback: progress_callback(5, "Estandarizando memoria en bloque...")
        
        df = df_maestro.copy()
        df['Clave_Agrupacion'] = df['Numero_Cliente'].replace("", np.nan)
        df['Clave_Agrupacion'] = np.where(df['Clave_Agrupacion'].isna(), df['Nombre'].astype(str) + "_" + df.index.astype(str), df['Clave_Agrupacion'])
        
        if progress_callback: progress_callback(15, "Agrupando clientes duplicados en alta velocidad...")
        
        # Misma Vectorización Rápida para cruzar datos de múltiples Excels
        for col in ['Nombre', 'Vendedor', 'Zona_Cruda']:
            df[col] = df[col].replace([r'^\s*$', 'nan', 'None'], np.nan, regex=True)
            df[col] = df.groupby('Clave_Agrupacion')[col].transform(lambda x: x.ffill().bfill()).fillna("")
            
        text_agg = df.groupby('Clave_Agrupacion')['Row_String'].apply(lambda x: ' | '.join(x.astype(str))).reset_index()
        
        df_agrupado = df.drop_duplicates(subset=['Clave_Agrupacion']).copy()
        df_agrupado = df_agrupado.drop(columns=['Row_String']).merge(text_agg, on='Clave_Agrupacion', how='left')
        
        datos_procesados = []
        total_filas = len(df_agrupado)
        paso_progreso = max(1, total_filas // 20)
        
        for idx, (_, row) in enumerate(df_agrupado.iterrows()):
            if progress_callback and total_filas > 0:
                if idx % paso_progreso == 0:
                    porcentaje_real = 15 + int((idx / total_filas) * 85)
                    progress_callback(porcentaje_real, f"Procesando cliente {idx} de {total_filas}...")

            n = str(row['Nombre']).strip()
            c = str(row['Numero_Cliente']).strip()
            v = str(row['Vendedor']).strip()
            texto_total = str(row['Row_String'])
            
            # EXTRACCIÓN MAESTRA
            telefonos_encontrados = separar_telefonos(texto_total)
            zona_enriquecida = extraer_zona_inteligente(texto_total, row.get('Zona_Cruda', ''))
            
            # Filtros de basura
            if n == "" and c.startswith("SinID_") and len(telefonos_encontrados) == 0: continue
            if "cód." in n.lower() or "fecha:" in n.lower() or "hoja:" in n.lower() or "wood tools" in n.lower(): continue
            if "clientes habilitados" in n.lower() or "ordenado por" in n.lower(): continue
            
            nom_f = n if n != "" else "Cliente Sin Nombre"
            vend_f = v if v != "" else "Desconocido"
            num_cli = c if not c.startswith("SinID_") else ""
            
            registro = {
                'Nombre': nom_f,
                'Número de cliente': num_cli,
                'Zona del cliente': zona_enriquecida,
                'Vendedor': vend_f,
                'Primer número': telefonos_encontrados[0] if len(telefonos_encontrados) > 0 else "",
                'Segundo número': telefonos_encontrados[1] if len(telefonos_encontrados) > 1 else "",
                'Tercer número': telefonos_encontrados[2] if len(telefonos_encontrados) > 2 else "",
                'Cuarto número': telefonos_encontrados[3] if len(telefonos_encontrados) > 3 else "",
                'Quinto número': telefonos_encontrados[4] if len(telefonos_encontrados) > 4 else ""
            }
            datos_procesados.append(registro)
            
        if progress_callback: progress_callback(100, "¡Cruce finalizado!")
        return pd.DataFrame(datos_procesados)
    except Exception as e:
        raise RuntimeError(f"Falla en el motor de cruce: {str(e)}")

def guardar_excel(df_final, ruta_guardar):
    df_final.to_excel(ruta_guardar, index=False)