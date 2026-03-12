import pandas as pd
import numpy as np
import os
import re
import sqlite3
import json
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
# CONFIGURACIÓN DE CELULARES Y VÍNCULOS ZONA-VENDEDOR
# ==========================================
ARCHIVO_VEND = "vendedores_config.json"
ARCHIVO_VINCULOS = "vinculos_zonas.json"

def cargar_mapa_vendedores():
    defaults = {
        "0": "1145394279 o 1165630406", 
        "1": "1157528428",
        "01": "1157528428",
        "302": "1157528428",
        "40": "1157528427",
        "15": "1157528427",
        "18": "1145640940",
        "16": "1145640831",
        "44": "1156321012",
        "4": "1156321012",
        "04": "1156321012",
        "9": "1153455274",
        "09": "1153455274",
        "3": "1168457778",
        "03": "1168457778",
        "5": "1164591316",
        "05": "1164591316"
    }
    if not os.path.exists(ARCHIVO_VEND):
        try:
            with open(ARCHIVO_VEND, 'w', encoding='utf-8') as f:
                json.dump(defaults, f, indent=4)
        except: pass
        return defaults
    else:
        try:
            with open(ARCHIVO_VEND, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return defaults

def guardar_mapa_vendedores(nuevo_mapa):
    try:
        with open(ARCHIVO_VEND, 'w', encoding='utf-8') as f:
            json.dump(nuevo_mapa, f, indent=4)
        return True
    except:
        return False

def cargar_vinculos_zonas():
    defaults = {"137": "03"} # Ejemplo de fábrica
    if not os.path.exists(ARCHIVO_VINCULOS):
        try:
            with open(ARCHIVO_VINCULOS, 'w', encoding='utf-8') as f:
                json.dump(defaults, f, indent=4)
        except: pass
        return defaults
    else:
        try:
            with open(ARCHIVO_VINCULOS, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return defaults

def guardar_vinculos_zonas(nuevo_mapa):
    try:
        with open(ARCHIVO_VINCULOS, 'w', encoding='utf-8') as f:
            json.dump(nuevo_mapa, f, indent=4)
        return True
    except: return False

# ==========================================
# DICCIONARIO DE ZONAS
# ==========================================
MAPA_ZONAS = {
    '101': 'ZONA NORTE',
    '102': 'QUILMES',
    '103': 'LA PLATA / ENSENADA / BERISSO',
    '104': 'ZONA SUR',
    '107': 'ZONA OESTE',
    '110': 'ZONA CAPITAL',
    '115': 'RUTA 29 / SAN VICENTE',
    '120': 'RUTA 2',
    '122': 'SUR II (BARILOCHE, CHUBUT, STA CRUZ, T DEL FUEGO)',
    '124': 'LUJAN / MERCEDES',
    '130': 'RUTA 5 (TRENQUE LAUQUEN)',
    '132': 'RUTA 3 / BAHIA BLANCA',
    '136': 'ENTRE RIOS',
    '137': 'CORDOBA',
    '140': 'RUTA 8',
    '141': 'RUTA 7 (CHACABUCO)',
    '142': 'SANTA FE / ROSARIO',
    '143': 'MESOPOTAMIA (ENTRE RIOS Y CORRIENTES)',
    '144': 'RUTA 5 Y 7 (BRAGADO / SALTO)',
    '146': 'NOROESTE (TUCUMAN - SALTA - JUJUY - CATAMARCA)',
    '148': 'RUTA 3 (TANDIL / OLAVARRIA / AZUL)',
    '149': 'ESPERANZA / RAFAELA',
    '150': 'CUYO (MENDOZA - SAN JUAN - SAN LUIS - LA RIOJA)',
    '151': 'CHACO / FORMOSA',
    '152': 'MISIONES / CORRIENTES',
    '155': 'VALLE RIO NEGRO / NEUQUEN',
    '156': 'MAR DEL PLATA',
    '301': 'EXTERIOR'
}

def extraer_zona_inteligente(texto_fila, zona_cruda):
    texto = (str(texto_fila) + " | " + str(zona_cruda)).upper()
    matches = re.findall(r'\b(1[0-5]\d|301)\b', texto)
    for m in matches:
        if m in MAPA_ZONAS: return f"{m} | {MAPA_ZONAS[m]}"
            
    for num, desc in MAPA_ZONAS.items():
        clave = desc.split('(')[0].strip().upper()
        if clave in texto and clave not in ["ZONA", "RUTA", "ZONA SUR", "ZONA NORTE"]: return f"{num} | {desc}"
            
    if "ZONA SUR" in texto: return "102 o 104 | ZONA SUR"
    if "ZONA NORTE" in texto: return "101 | ZONA NORTE"
    
    zc = str(zona_cruda).strip()
    if zc and zc.lower() != 'nan': return zc
    return "Desconocida"

def extraer_vendedor_inteligente(texto_crudo, vendedor_actual, zona_o_cobrador):
    v = str(vendedor_actual).strip().upper()
    z = str(zona_o_cobrador).strip().upper()
    texto_completo = (str(texto_crudo) + " " + v + " " + z).upper()
    
    # 0. MAPA DINÁMICO DE ZONAS (Si detecta una zona, la asocia al vendedor guardado en tu panel)
    vinculos = cargar_vinculos_zonas()
    match_zona = re.search(r'\b(1[0-5]\d|301)\b', texto_completo)
    if match_zona:
        num_zona = match_zona.group(1)
        vend_asignado = str(vinculos.get(num_zona, "")).strip()
        if vend_asignado and vend_asignado != "0":
            return vend_asignado
    
    # 1. Buscamos nombres literales en la bolsa de texto por si quedaron escritos a mano
    if re.search(r'\b(JORGE)\b', texto_completo): return "18"
    if re.search(r'\b(ROBERTO)\b', texto_completo): return "05"
    if re.search(r'\b(ALAN)\b', texto_completo): return "44"
    if re.search(r'\b(LUCAS)\b', texto_completo): return "16"
    if re.search(r'\b(NICOLAS|NICO)\b', texto_completo): return "40"
    if re.search(r'\b(EZEQUIEL|EZE)\b', texto_completo): return "09"
    if re.search(r'\b(LUIS)\b', texto_completo): return "03"
    if re.search(r'\b(EMMANUEL|EMMA)\b', texto_completo): return "1"
    if re.search(r'\b(VALENTIN|VALENTÍN|CARLOS)\b', texto_completo): return "0"
    
    # 2. Análisis cruzado: si "Vendedor" está vacío o en 0, pero en "Cobrador/Zona" hay un número
    candidatos = [v, z]
    for dato in candidatos:
        if dato == "18": return "18" 
        if dato == "05" or dato == "5": return "05" 
        if dato == "44" or dato == "04" or dato == "4": return "44" 
        if dato == "16": return "16" 
        if dato == "40" or dato == "15": return "40" 
        if dato == "09" or dato == "9": return "09" 
        if dato == "03" or dato == "3": return "03" 
        if dato == "1": return "1" 
        if dato == "302/1": return "302/1"
        if dato == "40/15": return "40/15"

    # 3. Si viene un código compuesto especial en el texto (Ej: "302/1", "40/15")
    match_compuesto = re.search(r'\b(\d+/\d+)\b', texto_completo)
    if match_compuesto:
        return match_compuesto.group(1)
        
    # 4. Si encontramos un número incrustado en el texto de la columna Vendedor (ignorar el 0 por default)
    if re.match(r'^[\d]+$', v) and v != "0":
        return v
        
    match_num = re.search(r'\b(\d+)\b', v)
    if match_num and match_num.group(1) != "0":
        return match_num.group(1)
        
    return "0"

# ==========================================
# LÓGICA DE LIMPIEZA Y EXTRACCIÓN
# ==========================================
def separar_telefonos(texto_crudo):
    if pd.isna(texto_crudo): return []
    texto = str(texto_crudo).strip()
    texto = re.sub(r'\b\d{2}-\d{8}-\d{1}\b', '', texto)
    texto = re.sub(r'\b\d{2}/\d{2}/\d{4}\b', '', texto)
    partes = re.split(r'//|/|\*|_|cel:?|tel:?|móvil:?|movil:?|contacto:?|;|,|\|', texto, flags=re.IGNORECASE)
    telefonos_limpios = []
    for parte in partes:
        num_puro = ''.join(filter(str.isdigit, parte))
        if num_puro.startswith("000"): continue
        if len(num_puro) >= 8 and len(num_puro) <= 15:
            telefonos_limpios.append(num_puro)
    vistos = set()
    return [x for x in telefonos_limpios if not (x in vistos or vistos.add(x))]

def estandarizar_columnas(df):
    cols_str = [str(c).lower().strip() for c in df.columns]
    mapa = {}
    asignados = set() 
    for original, minuscula in zip(df.columns, cols_str):
        nuevo_nombre = None
        if any(p in minuscula for p in ['vend', 'corredor', 'rep', 'agente']): nuevo_nombre = 'Vendedor'
        elif any(p in minuscula for p in ['zona', 'locali', 'ciudad', 'ubic', 'direc', 'cobr', 'cobrador']): nuevo_nombre = 'Zona_Cruda'
        elif any(p in minuscula for p in ['tel', 'cel', 'móvil', 'movil', 'contacto']): nuevo_nombre = 'Telefonos_Raw'
        elif any(p in minuscula for p in ['cód', 'cod', 'nro', 'código', 'id']): nuevo_nombre = 'Numero_Cliente'
        elif any(p in minuscula for p in ['nombre', 'cliente', 'razon', 'razón', 'social']): nuevo_nombre = 'Nombre'
        if nuevo_nombre and nuevo_nombre not in asignados:
            mapa[original] = nuevo_nombre
            asignados.add(nuevo_nombre)
    df = df.rename(columns=mapa)
    df = df.loc[:, ~df.columns.duplicated()].copy() 
    return df

def procesar_un_archivo(ruta):
    try:
        if ruta.endswith('.csv'): 
            dfs_to_process = [pd.read_csv(ruta, dtype=str, header=None)]
        else: 
            xls = pd.ExcelFile(ruta)
            dfs_to_process = [pd.read_excel(xls, sheet_name=s, dtype=str, header=None) for s in xls.sheet_names]
            
        df_agrupado_total = []
        total_filas = 0
        
        for df_temp in dfs_to_process:
            if df_temp.empty: continue
            best_row = -1
            max_score = 0
            
            for idx, row in df_temp.head(25).iterrows():
                row_str = " ".join(row.fillna("").astype(str)).lower()
                if "-zzzz" in row_str or "-999" in row_str or "z.fiscal" in row_str or "ordenado por" in row_str:
                    continue
                score = 0
                non_empty_cols = 0
                for cell in row.fillna("").astype(str):
                    c_low = cell.lower().strip()
                    if c_low:
                        non_empty_cols += 1
                        if c_low in ['nombre', 'cliente', 'razon social', 'razón social', 'clientes']: score += 10
                        if c_low in ['cód.', 'cod.', 'cod', 'código', 'codigo', 'id', 'nro', 'cód']: score += 5
                        if c_low in ['teléfonos', 'telefono', 'tel', 'cel', 'celular', 'movil', 'contacto']: score += 5
                        if c_low in ['vendedor', 'vend', 'zona', 'localidad', 'direc', 'domicilio', 'cobrador', 'cobr.']: score += 3
                
                final_score = score * (1 if non_empty_cols > 2 else 0)
                if final_score > max_score:
                    max_score = final_score
                    best_row = idx
            
            if max_score >= 10: 
                header_idx = best_row
                df_temp.columns = df_temp.iloc[header_idx].fillna(pd.Series([f"Col_{i}" for i in range(len(df_temp.columns))])).astype(str)
                df_temp = df_temp.iloc[header_idx+1:].reset_index(drop=True)
            else:
                df_temp.columns = [f"Col_{i}" for i in range(len(df_temp.columns))]

            df_temp = df_temp.dropna(how='all') 
            if df_temp.empty: continue
            
            df_temp = estandarizar_columnas(df_temp)
            if 'Nombre' not in df_temp.columns: continue
            
            total_filas += len(df_temp)
            df_temp['Row_String'] = df_temp.apply(lambda row: ' | '.join(row.dropna().astype(str)), axis=1)
            
            for col in ['Nombre', 'Numero_Cliente', 'Zona_Cruda', 'Vendedor']:
                if col not in df_temp.columns: df_temp[col] = ""
                
            df_temp['Numero_Cliente'] = df_temp['Numero_Cliente'].replace(r'^\s*$', np.nan, regex=True).ffill()
            df_temp['Numero_Cliente'] = np.where(df_temp['Numero_Cliente'].isna(), "SinID_" + df_temp.index.astype(str), df_temp['Numero_Cliente'])
            
            for col in ['Nombre', 'Vendedor', 'Zona_Cruda']:
                df_temp[col] = df_temp[col].replace([r'^\s*$', 'nan', 'None'], np.nan, regex=True)
                df_temp[col] = df_temp.groupby('Numero_Cliente')[col].transform(lambda x: x.ffill().bfill())
                df_temp[col] = df_temp[col].fillna("")
                
            text_agg = df_temp.groupby('Numero_Cliente')['Row_String'].apply(lambda x: ' | '.join(x.astype(str))).reset_index()
            df_agrupado = df_temp.drop_duplicates(subset=['Numero_Cliente']).copy()
            df_agrupado = df_agrupado.drop(columns=['Row_String']).merge(text_agg, on='Numero_Cliente', how='left')
            df_agrupado_total.append(df_agrupado)
            
        if not df_agrupado_total: return pd.DataFrame(), 0
        df_final_archivo = pd.concat(df_agrupado_total, ignore_index=True)
        return df_final_archivo, total_filas
        
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
                    progress_callback(15 + int((idx / total_filas) * 85), f"Procesando cliente {idx} de {total_filas}...")

            n = str(row['Nombre']).strip()
            c = str(row['Numero_Cliente']).strip()
            v = str(row['Vendedor']).strip()
            zona_o_cobr = str(row.get('Zona_Cruda', '')).strip()
            texto_total = str(row['Row_String'])
            
            telefonos_encontrados = separar_telefonos(texto_total)
            zona_enriquecida = extraer_zona_inteligente(texto_total, zona_o_cobr)
            
            # --- ACÁ BUSCAMOS EL VENDEDOR ---
            vend_f = extraer_vendedor_inteligente(texto_total, v, zona_o_cobr)
            
            if n == "" and c.startswith("SinID_") and len(telefonos_encontrados) == 0: continue
            if "cód." in n.lower() or "fecha:" in n.lower() or "hoja:" in n.lower() or "wood tools" in n.lower(): continue
            if "clientes habilitados" in n.lower() or "ordenado por" in n.lower(): continue
            
            registro = {
                'Nombre': n if n != "" else "Cliente Sin Nombre",
                'Número de cliente': c if not c.startswith("SinID_") else "",
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