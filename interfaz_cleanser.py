import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import pandas as pd
from datetime import datetime

# IMPORTAMOS NUESTRO BACKEND
import backend_cleanser

class DataCleanserApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Motor de Unificación y Limpieza de Datos - WoodTools")
        self.root.geometry("1100x700")
        
        backend_cleanser.inicializar_db()
        self.df_maestro = pd.DataFrame()
        self.df_final = pd.DataFrame()
        
        # ==========================================
        # INTERFAZ GRÁFICA
        # ==========================================
        frame_top = tk.Frame(root, pady=15, padx=20, bg="#2b2b2b")
        frame_top.pack(fill="x")
        
        tk.Label(frame_top, text="🧠 CEREBRO DE DATOS WOODTOOLS", fg="white", bg="#2b2b2b", font=("Segoe UI", 16, "bold")).pack(side=tk.LEFT)
        
        frame_botones = tk.Frame(root, pady=10, padx=20)
        frame_botones.pack(fill="x")
        
        tk.Button(frame_botones, text="📂 Cargar Archivo Excel Único", command=self.cargar_archivo_individual, bg="#4CAF50", fg="white", font=("bold", 10)).pack(side=tk.LEFT, padx=5)
        tk.Button(frame_botones, text="📁 Rastrear Carpeta Completa (Windows)", command=self.cargar_carpeta_windows, bg="#FF9800", fg="white", font=("bold", 10)).pack(side=tk.LEFT, padx=5)
        tk.Button(frame_botones, text="🧹 Cruzar Datos y Eliminar Duplicados", command=self.cruzar_datos, bg="#2196F3", fg="white", font=("bold", 10)).pack(side=tk.LEFT, padx=30)
        tk.Button(frame_botones, text="📥 EXPORTAR BASE FINAL", command=self.exportar_excel, bg="#E91E63", fg="white", font=("bold", 10)).pack(side=tk.RIGHT, padx=5)

        # Notebook (Pestañas)
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill="both", expand=True, padx=20, pady=10)
        
        self.tab_datos = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_datos, text="📊 Base de Datos en Memoria")
        
        self.tab_historial = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_historial, text="🕒 Historial de Cargas")
        
        # Tabla de Datos
        self.tree = ttk.Treeview(self.tab_datos, columns=("Nom", "Nro", "Zona", "Vend", "Tels"), show="headings")
        self.tree.heading("Nom", text="Nombre"); self.tree.column("Nom", width=200)
        self.tree.heading("Nro", text="Nro Cliente"); self.tree.column("Nro", width=100)
        self.tree.heading("Zona", text="Zona"); self.tree.column("Zona", width=150)
        self.tree.heading("Vend", text="Vendedor"); self.tree.column("Vend", width=100)
        self.tree.heading("Tels", text="Teléfonos Extraídos"); self.tree.column("Tels", width=350)
        
        scroll = ttk.Scrollbar(self.tab_datos, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=scroll.set)
        scroll.pack(side="right", fill="y"); self.tree.pack(fill="both", expand=True)
        
        # Tabla de Historial
        self.tree_hist = ttk.Treeview(self.tab_historial, columns=("ID", "Fec", "Tipo", "Ruta", "Regs"), show="headings")
        self.tree_hist.heading("ID", text="ID"); self.tree_hist.column("ID", width=50)
        self.tree_hist.heading("Fec", text="Fecha"); self.tree_hist.column("Fec", width=150)
        self.tree_hist.heading("Tipo", text="Tipo"); self.tree_hist.column("Tipo", width=100)
        self.tree_hist.heading("Ruta", text="Ruta de Origen"); self.tree_hist.column("Ruta", width=400)
        self.tree_hist.heading("Regs", text="Registros Extraídos"); self.tree_hist.column("Regs", width=150)
        self.tree_hist.pack(fill="both", expand=True)
        
        self.lbl_estado = tk.Label(root, text="Esperando datos...", fg="gray")
        self.lbl_estado.pack(side="bottom", pady=5)
        
        self.actualizar_tabla_historial()

    # ==========================================
    # EVENTOS DE LA INTERFAZ
    # ==========================================
    def cargar_archivo_individual(self):
        ruta = filedialog.askopenfilename(filetypes=[("Excel y CSV", "*.xlsx *.xls *.csv")])
        if not ruta: return
        self.ejecutar_carga([ruta], "Archivo Individual")

    def cargar_carpeta_windows(self):
        carpeta = filedialog.askdirectory(title="Selecciona la carpeta principal de Windows")
        if not carpeta: return
        
        self.lbl_estado.config(text="Rastreando carpeta en profundidad. Esto puede demorar...", fg="blue")
        self.root.update()
        
        archivos_encontrados = []
        for raiz, directorios, archivos in os.walk(carpeta):
            for arch in archivos:
                if arch.endswith(('.xlsx', '.xls', '.csv')):
                    archivos_encontrados.append(os.path.join(raiz, arch))
                    
        if not archivos_encontrados:
            return messagebox.showinfo("Aviso", "No se encontraron archivos Excel o CSV en esa carpeta.")
            
        self.ejecutar_carga(archivos_encontrados, f"Carpeta ({len(archivos_encontrados)} archivos)")

    def ejecutar_carga(self, lista_rutas, tipo_carga):
        # LLAMAMOS AL BACKEND PARA PROCESAR
        df_nuevo, total_filas = backend_cleanser.procesar_archivos(lista_rutas)
        
        if not df_nuevo.empty:
            self.df_maestro = pd.concat([self.df_maestro, df_nuevo], ignore_index=True)
            
            ruta_historial = lista_rutas[0] if len(lista_rutas) == 1 else "Múltiples archivos rastreados"
            backend_cleanser.registrar_historial(tipo_carga, ruta_historial, total_filas)
            
            self.actualizar_tabla_historial()
            self.actualizar_tabla_datos(cruza_finalizada=False)
            messagebox.showinfo("Éxito", f"Se cargaron {total_filas} registros en memoria.\nListos para cruzar.")

    def cruzar_datos(self):
        if self.df_maestro.empty:
            return messagebox.showwarning("Atención", "No hay datos cargados para cruzar.")
            
        self.lbl_estado.config(text="Procesando cruce de datos y extracción de teléfonos...", fg="blue")
        self.root.update()
        
        # LLAMAMOS AL BACKEND PARA CRUZAR Y LIMPIAR
        self.df_final = backend_cleanser.procesar_cruce(self.df_maestro)
        
        self.actualizar_tabla_datos(cruza_finalizada=True)
        messagebox.showinfo("Cruce Finalizado", f"Se unificaron los datos.\nBase optimizada: {len(self.df_final)} clientes únicos.")

    def exportar_excel(self):
        if self.df_final.empty:
            return messagebox.showerror("Error", "Primero debes cargar archivos y presionar el botón de 'Cruzar Datos' para generar la base limpia.")
            
        ruta_guardar = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            title="Guardar Base de Datos Optimizada",
            initialfile=f"Base_WoodTools_Optimizada_{datetime.now().strftime('%Y%m%d')}.xlsx"
        )
        
        if ruta_guardar:
            try:
                # LLAMAMOS AL BACKEND PARA GUARDAR
                backend_cleanser.guardar_excel(self.df_final, ruta_guardar)
                messagebox.showinfo("Éxito", f"Base de datos exportada perfectamente.\n\nYa está lista para subirla a tu CRM de WhatsApp.")
                os.startfile(os.path.dirname(ruta_guardar))
            except Exception as e:
                messagebox.showerror("Error al guardar", str(e))

    def actualizar_tabla_datos(self, cruza_finalizada=False):
        for i in self.tree.get_children(): self.tree.delete(i)
        
        if cruza_finalizada:
            for _, row in self.df_final.iterrows():
                tels_str = " | ".join([t for t in [row['Primer número'], row['Segundo número'], row['Tercer número']] if t])
                self.tree.insert("", "end", values=(row['Nombre'], row['Número de cliente'], row['Zona del cliente'], row['Vendedor'], tels_str))
            self.lbl_estado.config(text=f"Base unificada y lista: {len(self.df_final)} registros únicos.", fg="green")
        else:
            for _, row in self.df_maestro.head(100).iterrows(): # Muestra preview
                self.tree.insert("", "end", values=(row.get('Nombre',''), row.get('Numero_Cliente',''), row.get('Zona_Cruda',''), row.get('Vendedor',''), row.get('Telefonos_Raw','')))
            self.lbl_estado.config(text=f"Registros crudos en memoria: {len(self.df_maestro)}. (Mostrando preview). Falta cruzar datos.", fg="orange")

    def actualizar_tabla_historial(self):
        for i in self.tree_hist.get_children(): self.tree_hist.delete(i)
        try:
            df_hist = backend_cleanser.obtener_historial()
            for _, row in df_hist.iterrows():
                self.tree_hist.insert("", "end", values=(row['id'], row['fecha'], row['tipo_carga'], row['ruta'], row['registros_encontrados']))
        except: pass

if __name__ == "__main__":
    root = tk.Tk()
    app = DataCleanserApp(root)
    root.mainloop()