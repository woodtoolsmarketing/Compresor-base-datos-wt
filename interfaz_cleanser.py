import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import sys
import time
import threading
import pandas as pd
from datetime import datetime
from PIL import Image, ImageTk

import backend_cleanser

def resource_path(relative_path):
    """Obtiene la ruta absoluta para que las imágenes funcionen adentro del .exe"""
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

class DataCleanserApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Motor de Unificación y Limpieza de Datos - WoodTools")
        self.root.geometry("1150x750")
        
        try:
            ruta_ico = resource_path(os.path.join("Imagenes", "logo.ico"))
            self.root.iconbitmap(ruta_ico)
        except Exception as e:
            pass

        backend_cleanser.inicializar_db()
        self.df_maestro = pd.DataFrame()
        self.df_final = pd.DataFrame()
        
        self.cola_rutas = []
        self.hilo_activo = False
        self.pausado = False
        self.cancelado = False
        
        frame_top = tk.Frame(root, pady=15, padx=20, bg="#2b2b2b")
        frame_top.pack(fill="x")
        tk.Label(frame_top, text="🧠 CEREBRO DE DATOS WOODTOOLS", fg="white", bg="#2b2b2b", font=("Segoe UI", 16, "bold")).pack(side=tk.LEFT)
        
        try:
            ruta_png = resource_path(os.path.join("Imagenes", "logo.png"))
            imagen_original = Image.open(ruta_png)
            imagen_redimensionada = imagen_original.resize((45, 45), Image.LANCZOS)
            self.logo_img = ImageTk.PhotoImage(imagen_redimensionada)
            tk.Label(frame_top, image=self.logo_img, bg="#2b2b2b").pack(side=tk.RIGHT)
        except Exception as e:
            pass
            
        frame_botones = tk.Frame(root, pady=10, padx=20)
        frame_botones.pack(fill="x")
        
        tk.Button(frame_botones, text="📂 1. Cargar Archivo", command=self.cargar_archivo_individual, bg="#4CAF50", fg="white", font=("bold", 10)).pack(side=tk.LEFT, padx=5)
        tk.Button(frame_botones, text="📁 1. Rastrear Carpeta", command=self.cargar_carpeta_windows, bg="#FF9800", fg="white", font=("bold", 10)).pack(side=tk.LEFT, padx=5)
        tk.Button(frame_botones, text="🧹 2. Forzar Cruce Manual", command=self.iniciar_cruce_fondo, bg="#2196F3", fg="white", font=("bold", 10)).pack(side=tk.LEFT, padx=30)
        
        tk.Button(frame_botones, text="📥 3. EXPORTAR BASE FINAL", command=self.exportar_excel, bg="#E91E63", fg="white", font=("bold", 10)).pack(side=tk.RIGHT, padx=5)
        
        # --- NUEVO BOTON PARA VINCULAR ZONAS ---
        tk.Button(frame_botones, text="🗺️ Vincular Zonas", command=self.abrir_config_zonas, bg="#9C27B0", fg="white", font=("bold", 10)).pack(side=tk.RIGHT, padx=5)
        tk.Button(frame_botones, text="⚙️ Configurar Celulares", command=self.abrir_config_vendedores, bg="#607D8B", fg="white", font=("bold", 10)).pack(side=tk.RIGHT, padx=10)

        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill="both", expand=True, padx=20, pady=10)
        
        self.tab_cola = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_cola, text="⏳ Cola de Archivos")
        
        self.tab_datos = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_datos, text="📊 Base de Datos Procesada")
        
        self.tab_historial = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_historial, text="🕒 Historial de Cargas")
        
        # PESTAÑA COLA
        frame_cola_controles = tk.Frame(self.tab_cola, pady=10)
        frame_cola_controles.pack(fill="x")
        
        tk.Label(frame_cola_controles, text="Seleccioná un elemento para quitarlo si te equivocaste:", font=("Arial", 10)).pack(side="left", padx=10)
        tk.Button(frame_cola_controles, text="❌ Quitar Seleccionado", command=self.quitar_archivo_cola, bg="#f44336", fg="white").pack(side="left", padx=5)
        tk.Button(frame_cola_controles, text="🗑️ Vaciar Cola", command=self.vaciar_cola, bg="#757575", fg="white").pack(side="left", padx=5)
        
        self.listbox_cola = tk.Listbox(self.tab_cola, selectmode=tk.SINGLE, font=("Arial", 10))
        scroll_cola = ttk.Scrollbar(self.tab_cola, orient="vertical", command=self.listbox_cola.yview)
        self.listbox_cola.configure(yscrollcommand=scroll_cola.set)
        scroll_cola.pack(side="right", fill="y")
        self.listbox_cola.pack(fill="both", expand=True, padx=10, pady=5)
        
        frame_proceso_accion = tk.Frame(self.tab_cola, pady=20, bg="#f5f5f5")
        frame_proceso_accion.pack(fill="x", side="bottom")
        self.btn_iniciar = tk.Button(frame_proceso_accion, text="▶ INICIAR LECTURA", command=self.iniciar_procesamiento_fondo, bg="#4CAF50", fg="white", font=("Segoe UI", 12, "bold"), width=30)
        self.btn_iniciar.pack(pady=10)
        
        # PESTAÑA BASE DE DATOS
        frame_tabla = tk.Frame(self.tab_datos)
        frame_tabla.pack(fill="both", expand=True, padx=10, pady=10)
        
        self.tree = ttk.Treeview(frame_tabla, columns=("Nom", "Nro", "Zona", "Vend", "InfoExtra"), show="headings")
        self.tree.heading("Nom", text="Nombre"); self.tree.column("Nom", width=200)
        self.tree.heading("Nro", text="Nro Cliente"); self.tree.column("Nro", width=100)
        self.tree.heading("Zona", text="Zona Cruda"); self.tree.column("Zona", width=150)
        self.tree.heading("Vend", text="Código Vendedor"); self.tree.column("Vend", width=120)
        self.tree.heading("InfoExtra", text="Data Extraída"); self.tree.column("InfoExtra", width=300)
        
        scroll = ttk.Scrollbar(frame_tabla, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=scroll.set)
        scroll.pack(side="right", fill="y")
        self.tree.pack(side="left", fill="both", expand=True)
        
        frame_edicion = tk.Frame(self.tab_datos, pady=5)
        frame_edicion.pack(fill="x", side="bottom")
        tk.Button(frame_edicion, text="✏️ Editar Fila Seleccionada", command=self.editar_cliente_ui, bg="#009688", fg="white", font=("Arial", 10, "bold")).pack(side="right", padx=20)
        tk.Label(frame_edicion, text="Hacé clic en un cliente de arriba y presioná el botón para corregir su vendedor a mano.", fg="gray").pack(side="right", padx=10)
        
        # PESTAÑA HISTORIAL
        self.tree_hist = ttk.Treeview(self.tab_historial, columns=("ID", "Fec", "Tipo", "Ruta", "Regs"), show="headings")
        self.tree_hist.heading("ID", text="ID"); self.tree_hist.column("ID", width=50)
        self.tree_hist.heading("Fec", text="Fecha"); self.tree_hist.column("Fec", width=150)
        self.tree_hist.heading("Tipo", text="Tipo"); self.tree_hist.column("Tipo", width=100)
        self.tree_hist.heading("Ruta", text="Ruta de Origen"); self.tree_hist.column("Ruta", width=400)
        self.tree_hist.heading("Regs", text="Registros Extraídos"); self.tree_hist.column("Regs", width=150)
        self.tree_hist.pack(fill="both", expand=True)
        
        self.lbl_estado_principal = tk.Label(root, text="Esperando instrucciones...", fg="gray", font=("Arial", 10, "bold"))
        self.lbl_estado_principal.pack(side="bottom", pady=5)
        self.actualizar_tabla_historial()

    # ==========================================
    # MENU DE CONFIGURACIÓN DE ZONAS (NUEVO)
    # ==========================================
    def abrir_config_zonas(self):
        vent_conf = tk.Toplevel(self.root)
        vent_conf.title("Vincular Zonas a Vendedores")
        vent_conf.geometry("450x600")
        vent_conf.transient(self.root)
        vent_conf.grab_set()

        tk.Label(vent_conf, text="Asigná qué vendedor maneja cada Zona", font=("Segoe UI", 12, "bold"), fg="#2b2b2b").pack(pady=10)
        tk.Label(vent_conf, text="Ingresá el CÓDIGO del vendedor (ej: 18, 05, 44)", fg="gray").pack()
        
        frame_lista = tk.Frame(vent_conf)
        frame_lista.pack(fill="both", expand=True, padx=10, pady=5)
        
        canvas = tk.Canvas(frame_lista, borderwidth=0, highlightthickness=0)
        scrollbar = ttk.Scrollbar(frame_lista, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas)

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        vinculos = backend_cleanser.cargar_vinculos_zonas()
        self.entradas_zonas = {}
        
        tk.Label(scrollable_frame, text="Zona del País", font=("Arial", 9, "bold")).grid(row=0, column=0, padx=5, pady=5, sticky="w")
        tk.Label(scrollable_frame, text="Código Vendedor", font=("Arial", 9, "bold")).grid(row=0, column=1, padx=5, pady=5)
        
        for i, (num_zona, desc_zona) in enumerate(backend_cleanser.MAPA_ZONAS.items(), start=1):
            tk.Label(scrollable_frame, text=f"{num_zona} - {desc_zona}").grid(row=i, column=0, padx=5, pady=5, sticky="w")
            ent = tk.Entry(scrollable_frame, width=15)
            val_actual = vinculos.get(num_zona, "")
            ent.insert(0, val_actual)
            ent.grid(row=i, column=1, padx=5, pady=5)
            self.entradas_zonas[num_zona] = ent
            
        def guardar():
            nuevo_mapa = {z: ent.get().strip() for z, ent in self.entradas_zonas.items() if ent.get().strip()}
            if backend_cleanser.guardar_vinculos_zonas(nuevo_mapa):
                messagebox.showinfo("Éxito", "Vínculos guardados.\nAhora el programa sabrá a quién le pertenece cada zona.", parent=vent_conf)
                vent_conf.destroy()
            else:
                messagebox.showerror("Error", "No se pudo guardar la configuración.", parent=vent_conf)
                
        tk.Button(vent_conf, text="💾 Guardar Vínculos", command=guardar, bg="#4CAF50", fg="white", font=("bold", 11)).pack(pady=10)

    # ==========================================
    # MENU DE CONFIGURACIÓN DE VENDEDORES (GENERAL)
    # ==========================================
    def abrir_config_vendedores(self):
        vent_conf = tk.Toplevel(self.root)
        vent_conf.title("Base de Datos: Vendedores")
        vent_conf.geometry("400x480")
        vent_conf.transient(self.root)
        vent_conf.grab_set()

        tk.Label(vent_conf, text="Directorio de Celulares para el Bot", font=("Segoe UI", 12, "bold"), fg="#2b2b2b").pack(pady=10)
        
        frame_lista = tk.Frame(vent_conf)
        frame_lista.pack(fill="both", expand=True, padx=10, pady=5)
        
        canvas = tk.Canvas(frame_lista, borderwidth=0, highlightthickness=0)
        scrollbar = ttk.Scrollbar(frame_lista, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas)

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        mapa = backend_cleanser.cargar_mapa_vendedores()
        self.entradas_vend = {}
        
        tk.Label(scrollable_frame, text="Código Exportado", font=("Arial", 9, "bold")).grid(row=0, column=0, padx=5, pady=5)
        tk.Label(scrollable_frame, text="Celular Real", font=("Arial", 9, "bold")).grid(row=0, column=1, padx=5, pady=5)
        
        for i, (cod, tel) in enumerate(mapa.items(), start=1):
            tk.Label(scrollable_frame, text=f"Código [{cod}]:").grid(row=i, column=0, padx=5, pady=5, sticky="e")
            ent = tk.Entry(scrollable_frame, width=25)
            ent.insert(0, str(tel))
            ent.grid(row=i, column=1, padx=5, pady=5)
            self.entradas_vend[cod] = ent
            
        def guardar():
            nuevo_mapa = {cod: ent.get().strip() for cod, ent in self.entradas_vend.items()}
            if backend_cleanser.guardar_mapa_vendedores(nuevo_mapa):
                messagebox.showinfo("Éxito", "Directorio guardado correctamente.\nTu script de WhatsApp leerá este archivo.", parent=vent_conf)
                vent_conf.destroy()
            else:
                messagebox.showerror("Error", "No se pudo guardar la configuración.", parent=vent_conf)
                
        tk.Button(vent_conf, text="💾 Guardar Directorio", command=guardar, bg="#4CAF50", fg="white", font=("bold", 11)).pack(pady=10)

    # ==========================================
    # EDICIÓN INDIVIDUAL DE UN CLIENTE
    # ==========================================
    def editar_cliente_ui(self):
        if self.df_final.empty:
            return messagebox.showwarning("Atención", "Primero debes presionar 'Cruzar Datos' (Paso 2) para poder editar a los clientes finales.")
            
        seleccion = self.tree.selection()
        if not seleccion:
            return messagebox.showwarning("Atención", "Hacé clic en un cliente de la tabla para seleccionarlo y luego presioná Editar.")
            
        item_id = seleccion[0]
        valores = self.tree.item(item_id, "values")
        
        vent_edit = tk.Toplevel(self.root)
        vent_edit.title("Editar Cliente Específico")
        vent_edit.geometry("450x300")
        vent_edit.transient(self.root)
        vent_edit.grab_set()
        
        tk.Label(vent_edit, text="Corregir datos del cliente:", font=("Segoe UI", 12, "bold")).grid(row=0, column=0, columnspan=2, pady=10)
        
        campos = ["Nombre:", "Nro Cliente:", "Zona:", "Código Vendedor:", "Teléfonos:"]
        entradas = []
        
        for i, campo in enumerate(campos):
            tk.Label(vent_edit, text=campo, font=("Arial", 10)).grid(row=i+1, column=0, padx=10, pady=5, sticky="e")
            ent = tk.Entry(vent_edit, width=40)
            ent.insert(0, str(valores[i]))
            ent.grid(row=i+1, column=1, padx=10, pady=5)
            entradas.append(ent)
            
        def guardar_edicion():
            nuevos_val = [e.get().strip() for e in entradas]
            self.tree.item(item_id, values=nuevos_val)
            
            nro_original = valores[1]
            idx_list = self.df_final.index[self.df_final['Número de cliente'] == nro_original].tolist()
            
            if idx_list:
                idx = idx_list[0]
                self.df_final.at[idx, 'Nombre'] = nuevos_val[0]
                self.df_final.at[idx, 'Número de cliente'] = nuevos_val[1]
                self.df_final.at[idx, 'Zona del cliente'] = nuevos_val[2]
                self.df_final.at[idx, 'Vendedor'] = nuevos_val[3] 
                
                tels = [t.strip() for t in nuevos_val[4].split('|') if t.strip()]
                self.df_final.at[idx, 'Primer número'] = tels[0] if len(tels) > 0 else ""
                self.df_final.at[idx, 'Segundo número'] = tels[1] if len(tels) > 1 else ""
                self.df_final.at[idx, 'Tercer número'] = tels[2] if len(tels) > 2 else ""
                self.df_final.at[idx, 'Cuarto número'] = tels[3] if len(tels) > 3 else ""
                self.df_final.at[idx, 'Quinto número'] = tels[4] if len(tels) > 4 else ""
                
            vent_edit.destroy()
            messagebox.showinfo("Guardado", f"El cliente {nuevos_val[0]} fue actualizado con éxito.\nSe guardó con el código de vendedor: {nuevos_val[3]}.", parent=self.root)
            
        tk.Button(vent_edit, text="💾 Guardar Cambios", command=guardar_edicion, bg="#4CAF50", fg="white", font=("bold", 10)).grid(row=len(campos)+1, column=0, columnspan=2, pady=15)

    # ==========================================
    # LÓGICA DE LA COLA
    # ==========================================
    def cargar_archivo_individual(self):
        ruta = filedialog.askopenfilename(filetypes=[("Excel y CSV", "*.xlsx *.xls *.csv")])
        if ruta:
            self.cola_rutas.append(ruta)
            self.refrescar_listbox_cola()
            self.notebook.select(self.tab_cola)
            self.lbl_estado_principal.config(text=f"Archivo agregado a la cola.", fg="green")

    def cargar_carpeta_windows(self):
        carpeta = filedialog.askdirectory(title="Selecciona la carpeta principal de Windows")
        if not carpeta: return
        self.cola_rutas.append(carpeta)
        self.refrescar_listbox_cola()
        self.notebook.select(self.tab_cola)
        self.lbl_estado_principal.config(text=f"Carpeta agendada.", fg="green")

    def quitar_archivo_cola(self):
        seleccion = self.listbox_cola.curselection()
        if not seleccion: return
        indice = seleccion[0]
        del self.cola_rutas[indice]
        self.refrescar_listbox_cola()

    def vaciar_cola(self):
        self.cola_rutas.clear()
        self.refrescar_listbox_cola()

    def refrescar_listbox_cola(self):
        self.listbox_cola.delete(0, tk.END)
        for ruta in self.cola_rutas:
            self.listbox_cola.insert(tk.END, os.path.basename(ruta))
        if self.cola_rutas:
            self.btn_iniciar.config(state="normal")
        else:
            self.btn_iniciar.config(state="disabled")

    # ==========================================
    # PROCESAMIENTO
    # ==========================================
    def abrir_popup_lectura(self):
        self.vent_progreso = tk.Toplevel(self.root)
        self.vent_progreso.title("Procesando Archivos...")
        self.vent_progreso.geometry("550x300")
        self.vent_progreso.resizable(False, False)
        self.vent_progreso.transient(self.root)
        self.vent_progreso.grab_set()
        self.vent_progreso.protocol("WM_DELETE_WINDOW", self.cancelar_procesamiento)

        tk.Label(self.vent_progreso, text="Extrayendo datos de la cola...", font=("Segoe UI", 12, "bold"), fg="#333").pack(pady=10)
        self.lbl_porcentaje = tk.Label(self.vent_progreso, text="0%", font=("Segoe UI", 26, "bold"), fg="#2196F3")
        self.lbl_porcentaje.pack()

        self.var_progreso = tk.DoubleVar()
        self.barra_progreso = ttk.Progressbar(self.vent_progreso, variable=self.var_progreso, maximum=100, length=450)
        self.barra_progreso.pack(pady=5)
        
        self.lbl_filas_memoria = tk.Label(self.vent_progreso, text="Filas cargadas: 0", font=("Arial", 10, "bold"), fg="green")
        self.lbl_filas_memoria.pack(pady=2)

        self.lbl_archivo_actual = tk.Label(self.vent_progreso, text="Iniciando...", font=("Arial", 9), fg="gray")
        self.lbl_archivo_actual.pack(pady=5)

        frame_btns = tk.Frame(self.vent_progreso)
        frame_btns.pack(pady=10)
        self.btn_pausa = tk.Button(frame_btns, text="⏸ Pausar", command=self.alternar_pausa, bg="#FFC107", font=("bold", 10), width=12)
        self.btn_pausa.pack(side="left", padx=10)
        self.btn_cancelar = tk.Button(frame_btns, text="⏹ Cancelar", command=self.cancelar_procesamiento, bg="#F44336", fg="white", font=("bold", 10), width=12)
        self.btn_cancelar.pack(side="left", padx=10)

    def iniciar_procesamiento_fondo(self):
        if not self.cola_rutas: return
        if self.hilo_activo: return
        self.hilo_activo = True
        self.pausado = False
        self.cancelado = False
        self.btn_iniciar.config(state="disabled")
        self.abrir_popup_lectura()
        threading.Thread(target=self._trabajador_procesamiento).start()

    def alternar_pausa(self):
        if not self.hilo_activo: return
        self.pausado = not self.pausado
        if self.pausado:
            self.btn_pausa.config(text="▶ Reanudar", bg="#8BC34A")
            self.lbl_archivo_actual.config(text="⏸ PAUSADO - Esperando para continuar...", fg="orange")
        else:
            self.btn_pausa.config(text="⏸ Pausar", bg="#FFC107")
            self.lbl_archivo_actual.config(text="Reanudando...", fg="blue")

    def cancelar_procesamiento(self):
        if not self.hilo_activo: return
        respuesta = messagebox.askyesno("Confirmar Cancelación", "¿Estás seguro de detener la carga?\nSe guardará en memoria TODO lo que se haya logrado extraer.", parent=self.vent_progreso)
        if respuesta:
            self.cancelado = True
            self.pausado = False 
            self.lbl_archivo_actual.config(text="Cancelando proceso...", fg="red")
            self.btn_pausa.config(state="disabled")
            self.btn_cancelar.config(state="disabled")

    def _quitar_primero_y_refrescar(self):
        if self.cola_rutas:
            self.cola_rutas.pop(0)
            self.refrescar_listbox_cola()

    def _trabajador_procesamiento(self):
        filas_procesadas = 0
        archivos_exitosos = 0
        archivos_corruptos = 0
        
        def finalizar_ui(error_msg=None):
            self.hilo_activo = False
            self.btn_iniciar.config(state="normal")
            if hasattr(self, 'vent_progreso') and self.vent_progreso.winfo_exists():
                self.vent_progreso.destroy()
                
            self.actualizar_tabla_historial()
            self.actualizar_tabla_datos(cruza_finalizada=False)
            
            if error_msg:
                self.lbl_estado_principal.config(text="ERROR FATAL.", fg="red")
                messagebox.showerror("Error de Archivo", f"Se detuvo la lectura por un error crítico:\n\n{error_msg}")
            elif self.cancelado:
                self.lbl_estado_principal.config(text="PROCESO CANCELADO.", fg="red")
            
            # --- AUTO-CRUCE: Acá está la magia para que cruce solo después de cargar ---
            if not self.cancelado and not error_msg and not self.df_maestro.empty:
                self.root.after(500, self.iniciar_cruce_fondo)
                
        try:
            self.root.after(0, lambda: self.lbl_archivo_actual.config(text="Escaneando carpetas...", fg="blue"))
            rutas_expandidas = []
            PALABRAS_CLAVE = ['contacto', 'cliente', 'maestro', 'base', 'padron', 'datos', 'zona', 'giras', 'rutas']
            
            for ruta in self.cola_rutas:
                if os.path.isdir(ruta):
                    for raiz, directorios, archivos in os.walk(ruta):
                        for arch in archivos:
                            nombre_low = arch.lower()
                            if arch.endswith(('.xlsx', '.xls', '.csv')) and not arch.startswith('~$'):
                                if any(palabra in nombre_low for palabra in PALABRAS_CLAVE):
                                    rutas_expandidas.append(os.path.join(raiz, arch))
                else:
                    rutas_expandidas.append(ruta)
                    
            self.cola_rutas = rutas_expandidas
            self.root.after(0, self.refrescar_listbox_cola)
            
            if not self.cola_rutas:
                self.root.after(0, lambda: messagebox.showinfo("Filtro", "No se encontraron archivos válidos.", parent=self.vent_progreso))
                self.cancelado = True
            
            df_acumulado = []
            rutas_pendientes = list(self.cola_rutas)
            total_archivos = len(rutas_pendientes)
            archivos_procesados = 0
            
            for ruta_actual in rutas_pendientes:
                if self.cancelado: break
                while self.pausado:
                    time.sleep(0.5)
                    if self.cancelado: break
                if self.cancelado: break
                
                porcentaje = int((archivos_procesados / total_archivos) * 98) if total_archivos > 0 else 98
                nombre_arch = os.path.basename(ruta_actual)
                
                self.root.after(0, lambda p=porcentaje: self.var_progreso.set(p))
                self.root.after(0, lambda p=porcentaje: self.lbl_porcentaje.config(text=f"{p}%"))
                self.root.after(0, lambda n=nombre_arch: self.lbl_archivo_actual.config(text=f"Leyendo: {n}...", fg="blue"))
                
                df_temp, filas = backend_cleanser.procesar_un_archivo(ruta_actual)
                
                if not df_temp.empty:
                    df_acumulado.append(df_temp)
                    filas_procesadas += filas
                    archivos_exitosos += 1
                    self.root.after(0, lambda f=filas_procesadas: self.lbl_filas_memoria.config(text=f"Filas cargadas: {f}"))
                else:
                    archivos_corruptos += 1
                
                archivos_procesados += 1
                self.root.after(0, self._quitar_primero_y_refrescar)
                
            if df_acumulado:
                self.root.after(0, lambda: self.var_progreso.set(99))
                self.root.after(0, lambda: self.lbl_porcentaje.config(text="99%"))
                
                df_nuevo = pd.concat(df_acumulado, ignore_index=True)
                if not self.df_maestro.empty:
                    self.df_maestro = pd.concat([self.df_maestro, df_nuevo], ignore_index=True)
                else:
                    self.df_maestro = df_nuevo
                
                self.root.after(0, lambda: self.var_progreso.set(100))
                self.root.after(0, lambda: self.lbl_porcentaje.config(text="100%"))
                self.root.after(0, lambda: backend_cleanser.registrar_historial("Lote Procesado", f"{archivos_exitosos} archivos", filas_procesadas))
                
            self.root.after(0, finalizar_ui)
            
        except Exception as e:
            self.root.after(0, lambda err=str(e): finalizar_ui(err))

    # ==========================================
    # POPUP 2: CRUCE Y LIMPIEZA DE DATOS
    # ==========================================
    def abrir_popup_cruce(self):
        self.vent_cruce = tk.Toplevel(self.root)
        self.vent_cruce.title("Cruce de Datos Automático...")
        self.vent_cruce.geometry("550x250")
        self.vent_cruce.resizable(False, False)
        self.vent_cruce.transient(self.root)
        self.vent_cruce.grab_set()

        tk.Label(self.vent_cruce, text="Analizando y unificando clientes...", font=("Segoe UI", 12, "bold"), fg="#2196F3").pack(pady=15)
        self.lbl_porcentaje_cruce = tk.Label(self.vent_cruce, text="0%", font=("Segoe UI", 26, "bold"), fg="#FF9800")
        self.lbl_porcentaje_cruce.pack()

        self.var_progreso_cruce = tk.DoubleVar()
        self.barra_progreso_cruce = ttk.Progressbar(self.vent_cruce, variable=self.var_progreso_cruce, maximum=100, length=450)
        self.barra_progreso_cruce.pack(pady=10)

        self.lbl_estado_cruce = tk.Label(self.vent_cruce, text="Iniciando limpieza...", font=("Arial", 9), fg="gray")
        self.lbl_estado_cruce.pack(pady=5)

    def iniciar_cruce_fondo(self):
        if self.df_maestro.empty:
            return messagebox.showwarning("Atención", "No hay datos en memoria para cruzar. Primero leé algún archivo.")
            
        self.notebook.select(self.tab_datos)
        self.abrir_popup_cruce()
        threading.Thread(target=self._trabajador_cruce).start()

    def _trabajador_cruce(self):
        def actualizar_progreso_cruce(porcentaje, mensaje):
            self.root.after(0, lambda p=porcentaje: self.var_progreso_cruce.set(p))
            self.root.after(0, lambda p=porcentaje: self.lbl_porcentaje_cruce.config(text=f"{p}%"))
            self.root.after(0, lambda m=mensaje: self.lbl_estado_cruce.config(text=m, fg="blue"))

        try:
            self.df_final = backend_cleanser.procesar_cruce(self.df_maestro, actualizar_progreso_cruce)
            
            def finalizar_exito():
                if hasattr(self, 'vent_cruce') and self.vent_cruce.winfo_exists():
                    self.vent_cruce.destroy()
                self.actualizar_tabla_datos(cruza_finalizada=True)
                messagebox.showinfo("Proceso Exitoso", f"Se limpió la base perfectamente.\n\nPara que la Inteligencia asigne automáticamente a los vendedores según su zona, asegurate de tenerlas vinculadas en el botón violeta de '🗺️ Vincular Zonas'.")
                
            self.root.after(0, finalizar_exito)
            
        except Exception as e:
            def finalizar_error():
                if hasattr(self, 'vent_cruce') and self.vent_cruce.winfo_exists():
                    self.vent_cruce.destroy()
                self.lbl_estado_principal.config(text="Error durante el cruce de datos.", fg="red")
                messagebox.showerror("Error Interno", f"El motor de datos encontró un obstáculo:\n\n{str(e)}")
                
            self.root.after(0, finalizar_error)

    def actualizar_tabla_datos(self, cruza_finalizada=False):
        for i in self.tree.get_children(): self.tree.delete(i)
        
        LIMITE = 500 
        count = 0
        
        if cruza_finalizada:
            self.tree.heading("Zona", text="Zona Enriquecida")
            self.tree.heading("InfoExtra", text="Teléfonos Extraídos")
            for _, row in self.df_final.iterrows():
                if count >= LIMITE: break
                tels_str = " | ".join([t for t in [row['Primer número'], row['Segundo número'], row['Tercer número']] if t])
                self.tree.insert("", "end", values=(row['Nombre'], row['Número de cliente'], row['Zona del cliente'], row['Vendedor'], tels_str))
                count += 1
                
            texto_extra = f" (Mostrando primeros {LIMITE})" if len(self.df_final) > LIMITE else ""
            self.lbl_estado_principal.config(text=f"Base unificada y lista: {len(self.df_final)} registros únicos.{texto_extra}", fg="green")
        else:
            self.tree.heading("Zona", text="Zona Cruda (Memoria)")
            self.tree.heading("InfoExtra", text="Bolsa de Texto Crudo")
            for _, row in self.df_maestro.iterrows(): 
                if count >= LIMITE: break
                texto_bolsa = str(row.get('Row_String',''))[:80] + "..." if len(str(row.get('Row_String',''))) > 80 else str(row.get('Row_String',''))
                
                self.tree.insert("", "end", values=(row.get('Nombre',''), row.get('Numero_Cliente',''), row.get('Zona_Cruda',''), row.get('Vendedor',''), texto_bolsa))
                count += 1
                
            texto_extra = f" (Mostrando primeros {LIMITE})" if len(self.df_maestro) > LIMITE else ""
            self.lbl_estado_principal.config(text=f"Registros en memoria: {len(self.df_maestro)}.{texto_extra} Falta cruzar.", fg="orange")

    def exportar_excel(self):
        if self.df_final.empty:
            return messagebox.showerror("Error", "Primero debes esperar a que termine el 'Cruce de Datos' para generar la base limpia.")
            
        ruta_guardar = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            title="Guardar Base de Datos Optimizada",
            initialfile=f"Base_WoodTools_Optimizada_{datetime.now().strftime('%Y%m%d')}.xlsx"
        )
        
        if ruta_guardar:
            try:
                backend_cleanser.guardar_excel(self.df_final, ruta_guardar)
                messagebox.showinfo("Éxito", f"Base de datos exportada perfectamente.\n\nYa está lista para subirla a tu CRM de WhatsApp.")
                os.startfile(os.path.dirname(ruta_guardar))
            except Exception as e:
                messagebox.showerror("Error al guardar", str(e))

    def actualizar_tabla_historial(self):
        for i in self.tree_hist.get_children(): self.tree_hist.delete(i)
        try:
            df_hist = backend_cleanser.obtener_historial()
            for _, row in df_hist.iterrows():
                self.tree_hist.insert("", "end", values=(row['id'], row['fecha'], row['tipo_carga'], row['ruta'], row['registros_encontrados']))
        except: pass

if __name__ == "__main__":
    try:
        import ctypes
        myappid = 'woodtools.compresor.datos.1.0'
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
    except Exception:
        pass 
    
    root = tk.Tk()
    app = DataCleanserApp(root)
    root.mainloop()