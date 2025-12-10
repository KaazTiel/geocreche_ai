import os
import sys
import time
import json
import threading
import webbrowser
import datetime
from pathlib import Path
import customtkinter as ctk
from tkinter import messagebox
import pandas as pd
import psycopg2 

# --- PATH ADJUSTMENT (IMPORTANT) ---
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.append(str(project_root))

# Path to metadata and configuration files
META_FILE = project_root / "pipeline_meta.json"
DB_SETTINGS_FILE = project_root / "db_settings.json"

# --- PROJECT IMPORTS ---
from src.utils import config
from src.utils.env_loader import DB_CONFIG as DEFAULT_DB_CONFIG, DATA_CUTO, IDADE_MAX_DIAS

# Pipeline Steps
from src.processing.prenatals_filter import aplicar_filtro_validade
from src.processing.mother_preparation import preparar_maes_para_osm
from src.loaders.creches_loader import carregar_creches
from src.loaders.municipio_loader import carregar_municipio
from src.processing.osm_network import carregar_grafo_osm
from src.processing.nearest_creche import calcular_creche_mais_proxima
from src.reporting.mapas_cluster import gerar_mapa_clusters, gerar_mapa_tematico
from src.reporting.plots_previsao import gerar_previsoes_bairros
from src.reporting.report_builder import gerar_relatorio_final

# Visual Configuration
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

# --- DYNAMIC DATABASE LOADING FUNCTION ---
def carregar_prenatals_dinamico(db_config, query_config):
    """
    Loads data using dynamic GUI configurations.
    """
    try:
        conn = psycopg2.connect(**db_config)
        
        # Build query based on interface defined fields
        tabela = query_config.get("table", "prenatals")
        c_id = query_config.get("col_id", "prenatal_id")
        c_mother = query_config.get("col_mother", "mother_id")
        c_date = query_config.get("col_date", "dum")
        c_lat = query_config.get("col_lat", "lat")
        c_lon = query_config.get("col_lon", "lon")

        # Fixed aliases for the rest of the code
        sql = f"""
            SELECT 
                {c_id} as prenatal_id, 
                {c_mother} as mother_id, 
                {c_date} as dum, 
                {c_lat} as lat, 
                {c_lon} as lon 
            FROM {tabela};
        """
        
        print(f"[DB] Executing Query: {sql}")
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception as e:
        raise Exception(f"Error connecting or querying database: {e}")

class ConfigWindow(ctk.CTkToplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Connection Configuration")
        self.geometry("500x700")
        
        # --- ALTERAÇÃO 1: Habilitar redimensionamento e definir tamanho mínimo ---
        self.resizable(True, True) 
        self.minsize(450, 500)  # Evita que a janela fique inútil de tão pequena
        
        self.parent = parent
        
        # Load saved configs or use default
        self.current_config = self.carregar_json_config()
        self.criar_ui()

    def carregar_json_config(self):
        if os.path.exists(DB_SETTINGS_FILE):
            try:
                with open(DB_SETTINGS_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        
        return {
            "db": {k: "" for k in ["dbname", "user", "password", "host", "port"]},
            "query": {
                "table": "prenatals", "col_id": "prenatal_id", 
                "col_mother": "mother_id", "col_date": "dum", 
                "col_lat": "lat", "col_lon": "lon"
            }
        }

    def criar_ui(self):
        # --- ALTERAÇÃO 2: Container Principal Rolável ---
        # Isso permite que o conteúdo se adapte se a janela for redimensionada
        self.scroll_container = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.scroll_container.pack(fill="both", expand=True, padx=5, pady=5)

        # --- Database Section ---
        # O pai agora é self.scroll_container, não self
        self.frame_db = ctk.CTkFrame(self.scroll_container)
        self.frame_db.pack(pady=10, padx=10, fill="x")
        
        ctk.CTkLabel(self.frame_db, text="PostgreSQL Connection", font=("Arial", 16, "bold")).pack(pady=5)
        
        self.entries_db = {}
        fields_db = [
            ("Host", "host"), ("Port", "port"), 
            ("Database", "dbname"), ("User", "user"), 
            ("Password", "password")
        ]
        
        for label, key in fields_db:
            frm = ctk.CTkFrame(self.frame_db, fg_color="transparent")
            frm.pack(fill="x", pady=2)
            ctk.CTkLabel(frm, text=label, width=80, anchor="w").pack(side="left", padx=5)
            entry = ctk.CTkEntry(frm)
            entry.pack(side="left", fill="x", expand=True, padx=5)
            
            val = self.current_config["db"].get(key, "")
            if not val and key in DEFAULT_DB_CONFIG:
                val = DEFAULT_DB_CONFIG[key]
            
            entry.insert(0, str(val))
            if key == "password":
                entry.configure(show="*")
            
            self.entries_db[key] = entry

        # Test Button
        self.btn_test = ctk.CTkButton(self.frame_db, text="Test Connection and Fetch Tables", 
                                      fg_color="orange", text_color="black", hover_color="#ffaa00",
                                      command=self.testar_conexao_e_listar)
        self.btn_test.pack(pady=10, fill="x", padx=10)

        # --- Column Mapping Section ---
        # O pai agora é self.scroll_container
        self.frame_query = ctk.CTkFrame(self.scroll_container)
        self.frame_query.pack(pady=10, padx=10, fill="x")
        
        ctk.CTkLabel(self.frame_query, text="Table/Column Mapping", font=("Arial", 16, "bold")).pack(pady=5)
        
        self.combos_query = {}
        
        # 1. Table Field (Special, triggers column search)
        frm_tbl = ctk.CTkFrame(self.frame_query, fg_color="transparent")
        frm_tbl.pack(fill="x", pady=2)
        ctk.CTkLabel(frm_tbl, text="Table Name", width=120, anchor="w").pack(side="left", padx=5)
        
        self.combo_table = ctk.CTkComboBox(frm_tbl, values=[], command=self.on_tabela_selecionada)
        self.combo_table.pack(side="left", fill="x", expand=True, padx=5)
        
        # Set saved value
        saved_table = self.current_config["query"].get("table", "")
        if saved_table: 
            self.combo_table.set(saved_table)
            self.combo_table.configure(values=[saved_table]) # Temporary initial value

        self.combos_query["table"] = self.combo_table

        # 2. Column Fields
        fields_cols = [
            ("ID Column (PK)", "col_id"),
            ("Mother ID Column", "col_mother"),
            ("Date Column (LMP)", "col_date"),
            ("Latitude Column", "col_lat"),
            ("Longitude Column", "col_lon"),
        ]
        
        for label, key in fields_cols:
            frm = ctk.CTkFrame(self.frame_query, fg_color="transparent")
            frm.pack(fill="x", pady=2)
            ctk.CTkLabel(frm, text=label, width=120, anchor="w").pack(side="left", padx=5)
            
            combo = ctk.CTkComboBox(frm, values=[])
            combo.pack(side="left", fill="x", expand=True, padx=5)
            
            saved_val = self.current_config["query"].get(key, "")
            if saved_val:
                combo.set(saved_val)
                combo.configure(values=[saved_val])

            self.combos_query[key] = combo

        # Initially block
        self.toggle_query_inputs(state="disabled")

        # --- Save Button ---
        # ALTERAÇÃO 3: O botão Salvar fica fora do scroll (preso ao rodapé da janela)
        # para estar sempre visível.
        self.frame_footer = ctk.CTkFrame(self, fg_color="transparent")
        self.frame_footer.pack(fill="x", side="bottom", pady=10, padx=10)

        self.btn_save = ctk.CTkButton(self.frame_footer, text="SAVE CONFIGURATION", fg_color="green", hover_color="darkgreen",
                                      height=40, command=self.salvar_config)
        self.btn_save.pack(fill="x")

    def toggle_query_inputs(self, state):
        for combo in self.combos_query.values():
            combo.configure(state=state)

    def get_db_values(self):
        return {k: v.get() for k, v in self.entries_db.items()}

    def testar_conexao_e_listar(self):
        cfg = self.get_db_values()
        try:
            conn = psycopg2.connect(**cfg)
            cursor = conn.cursor()
            
            # Fetch public tables
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tabelas = [row[0] for row in cursor.fetchall()]
            tabelas.sort()
            
            conn.close()
            
            # Update Table Combobox
            self.combo_table.configure(values=tabelas)
            if tabelas:
                # If saved table is in list, keep it, else pick first
                atual = self.combo_table.get()
                if atual not in tabelas:
                    self.combo_table.set(tabelas[0])
                    # Trigger column update for first table
                    self.on_tabela_selecionada(tabelas[0])
                else:
                    self.on_tabela_selecionada(atual)
            
            messagebox.showinfo("Success", "Connection OK! Tables loaded.\nSelect the table to load columns.")
            self.toggle_query_inputs(state="normal")
            
        except Exception as e:
            messagebox.showerror("Connection Failed", f"Error: {e}")
            self.toggle_query_inputs(state="disabled")

    def on_tabela_selecionada(self, tabela_escolhida):
        if not tabela_escolhida: return
        
        cfg = self.get_db_values()
        try:
            conn = psycopg2.connect(**cfg)
            cursor = conn.cursor()
            
            # Fetch columns for chosen table
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (tabela_escolhida,))
            colunas = [row[0] for row in cursor.fetchall()]
            colunas.sort()
            conn.close()
            
            # Update all column combos
            for key, combo in self.combos_query.items():
                if key == "table": continue # Skip table combo
                
                combo.configure(values=colunas)
                
                # Try to keep old value if it exists in new list
                atual = combo.get()
                if atual not in colunas and colunas:
                    combo.set("") # Clear if invalid
        
        except Exception as e:
            print(f"Error fetching columns: {e}")

    def salvar_config(self):
        if self.combo_table.cget("state") == "disabled":
            if not messagebox.askyesno("Warning", "Connection has not been tested. Save anyway?"):
                return

        data = {
            "db": self.get_db_values(),
            "query": {k: v.get() for k, v in self.combos_query.items()}
        }
        
        try:
            with open(DB_SETTINGS_FILE, 'w') as f:
                json.dump(data, f, indent=4)
            messagebox.showinfo("Saved", "Configuration saved successfully!")
            self.destroy()
        except Exception as e:
            messagebox.showerror("Error", f"Error saving file: {e}")

class GeoCrecheApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        # Window Configuration
        self.title("Geo Creche AI - Pipeline Manager")
        self.geometry("700x600")
        self.resizable(False, False)

        # --- CPU RANGE CONFIGURATION ---
        # Dictionary: {Total Cores: (Green Limit, Yellow Limit)}
        # Green: Recommended (Smooth System)
        # Yellow: High Performance (Intensive Use)
        # Red: Above Yellow Limit (Max Load, Risk of UI freezing)
        self.cpu_ranges = {
            2:  (1, 1),      # Dual-core: 1 Safe, 2 Max
            4:  (2, 3),      # Quad-core: 1-2 Safe, 3 High, 4 Max
            6:  (4, 5),      # Hexa-core: 1-4 Safe, 5 High, 6 Max
            8:  (5, 7),      # Octa-core: 1-5 Safe, 6-7 High, 8 Max
            10: (7, 9),
            12: (8, 10),     # Ex: Ryzen 5900X / i7-12700
            16: (10, 14),    # Ex: Ryzen 5950X / i9
            24: (16, 22),    # Threadripper / Xeon
            32: (24, 30)
        }
        
        # Control Variables
        self.cpu_count = os.cpu_count() or 1
        
        # Define safe initial value (approx 60%)
        initial_cores = max(1, int(self.cpu_count * 0.6))
        self.num_processos_var = ctk.IntVar(value=initial_cores) 
        
        self.sincronizar_var = ctk.BooleanVar(value=False)
        self.output_path = config.OUTPUT_DIR / 'relatorio_final.html'

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.criar_interface()
        self.carregar_metadados()
        self.verificar_relatorio_existente()
        
        # Force initial visual update
        self.atualizar_label_cpu(self.num_processos_var.get())

    def criar_interface(self):
        # --- Main Frame ---
        self.main_frame = ctk.CTkFrame(self, corner_radius=15)
        self.main_frame.pack(pady=20, padx=20, fill="both", expand=True)

        # Header with Config Button
        self.frame_header = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.frame_header.pack(fill="x", pady=(20, 10), padx=20)

        self.label_titulo = ctk.CTkLabel(
            self.frame_header, 
            text="Geo Creche AI Pipeline", 
            font=ctk.CTkFont(size=24, weight="bold")
        )
        self.label_titulo.pack(side="left")

        # Config Button top right
        self.btn_config = ctk.CTkButton(
            self.frame_header, 
            text="⚙️ DB Config", 
            width=120,
            fg_color="#444", hover_color="#666",
            command=self.abrir_config_janela
        )
        self.btn_config.pack(side="right")

        # --- Settings Area ---
        self.frame_config = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.frame_config.pack(pady=10, padx=20, fill="x")

        # Process Slider Labels
        self.label_cpu = ctk.CTkLabel(
            self.frame_config, 
            text=f"Parallel Processes: {self.num_processos_var.get()}",
            font=ctk.CTkFont(size=14, weight="bold")
        )
        self.label_cpu.pack(anchor="w")
        
        self.label_cpu_hint = ctk.CTkLabel(
            self.frame_config,
            text="Status",
            font=ctk.CTkFont(size=12)
        )
        self.label_cpu_hint.pack(anchor="w", pady=(0, 5))

        # Slider
        self.slider_cpu = ctk.CTkSlider(
            self.frame_config, 
            from_=1, 
            to=self.cpu_count, 
            number_of_steps=self.cpu_count-1,
            variable=self.num_processos_var,
            command=self.atualizar_label_cpu
        )
        self.slider_cpu.pack(fill="x", pady=(5, 20))

        # Sync Switch
        self.switch_sync = ctk.CTkSwitch(
            self.frame_config, 
            text="Sync Database (Force Recalculation)", 
            variable=self.sincronizar_var,
            font=ctk.CTkFont(size=14)
        )
        self.switch_sync.pack(anchor="w", pady=10)

        # --- Status and Execution Area ---
        self.frame_status = ctk.CTkFrame(self.main_frame, fg_color=("gray90", "gray20"))
        self.frame_status.pack(pady=20, padx=20, fill="x")

        self.label_last_run = ctk.CTkLabel(
            self.frame_status, 
            text="Last run: Never",
            text_color="gray"
        )
        self.label_last_run.pack(pady=(10, 5))

        self.progressbar = ctk.CTkProgressBar(self.frame_status)
        self.progressbar.pack(fill="x", padx=20, pady=10)
        self.progressbar.set(0)

        self.label_status = ctk.CTkLabel(self.frame_status, text="Ready to start.")
        self.label_status.pack(pady=(0, 10))

        # --- Action Buttons ---
        self.frame_botoes = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.frame_botoes.pack(pady=10, padx=20, fill="x")

        self.btn_executar = ctk.CTkButton(
            self.frame_botoes, 
            text="START PIPELINE",
            height=45,
            font=ctk.CTkFont(size=15, weight="bold"),
            command=self.iniciar_thread_pipeline
        )
        self.btn_executar.pack(side="left", fill="x", expand=True, padx=(0, 10))

        self.btn_relatorio = ctk.CTkButton(
            self.frame_botoes, 
            text="OPEN REPORT",
            height=45,
            fg_color="green",
            hover_color="darkgreen",
            font=ctk.CTkFont(size=15, weight="bold"),
            command=self.abrir_relatorio,
            state="disabled"
        )
        self.btn_relatorio.pack(side="right", fill="x", expand=True, padx=(10, 0))

    # --- Interface Logic ---

    def atualizar_label_cpu(self, value):
        val = int(value)
        self.label_cpu.configure(text=f"Parallel Processes: {val}")
        
        # Determine limits based on total CPUs
        total = self.cpu_count
        
        # Generic fallback if processor is not in dictionary
        if total in self.cpu_ranges:
            limit_green, limit_yellow = self.cpu_ranges[total]
        else:
            limit_green = int(total * 0.6)
            limit_yellow = int(total * 0.85)

        # Define Colors and Texts
        if val <= limit_green:
            color = "#2CC985"  # Green (Safe)
            status_text = "🟢 Recommended (Smooth System)"
            bar_color = "standard" # Uses standard theme (blue) or green
        elif val <= limit_yellow:
            color = "#F4D03F"  # Yellow/Orange
            status_text = "🟡 High Performance (Intensive Use)"
            bar_color = "#F4D03F"
        else:
            color = "#FF5555"  # Red
            status_text = "🔴 Max Load (May freeze UI)"
            bar_color = "#FF5555"

        # Update UI
        self.label_cpu_hint.configure(text=status_text, text_color=color)
        
        # Optional: Change slider bar color to reflect danger
        if bar_color != "standard":
            self.slider_cpu.configure(progress_color=bar_color, button_color=bar_color, button_hover_color=bar_color)
        else:
            # Return to standard blue theme
            self.slider_cpu.configure(progress_color=["#3B8ED0", "#1F6AA5"], button_color=["#3B8ED0", "#1F6AA5"], button_hover_color=["#36719F", "#144870"])

    def abrir_config_janela(self):
        ConfigWindow(self)

    def carregar_metadados(self):
        if os.path.exists(META_FILE):
            try:
                with open(META_FILE, "r") as f:
                    data = json.load(f)
                    self.label_last_run.configure(text=f"Last run: {data.get('last_run', 'Unknown')}")
            except Exception:
                pass

    def salvar_metadados(self):
        now = datetime.datetime.now().strftime("%d/%m/%Y at %H:%M")
        try:
            with open(META_FILE, "w") as f:
                json.dump({"last_run": now}, f)
            self.label_last_run.configure(text=f"Last run: {now}")
        except Exception as e:
            print(f"[Warning] Could not save metadata: {e}")

    def verificar_relatorio_existente(self):
        if os.path.exists(self.output_path):
            self.btn_relatorio.configure(state="normal")
        else:
            self.btn_relatorio.configure(state="disabled")

    def abrir_relatorio(self):
        if os.path.exists(self.output_path):
            webbrowser.open(f"file://{os.path.abspath(self.output_path)}")
        else:
            messagebox.showerror("Error", "Report file not found.")

    # --- Execution Logic (Threading) ---

    def iniciar_thread_pipeline(self):
        self.btn_executar.configure(state="disabled", text="RUNNING...")
        self.btn_relatorio.configure(state="disabled")
        self.switch_sync.configure(state="disabled")
        self.slider_cpu.configure(state="disabled")
        self.btn_config.configure(state="disabled") 
        
        threading.Thread(target=self.rodar_pipeline_backend, daemon=True).start()

    def get_active_db_config(self):
        if os.path.exists(DB_SETTINGS_FILE):
            try:
                with open(DB_SETTINGS_FILE, 'r') as f:
                    data = json.load(f)
                    return data["db"], data["query"]
            except Exception as e:
                print(f"Error reading custom config: {e}")
        
        default_query = {
            "table": "prenatals", "col_id": "prenatal_id", 
            "col_mother": "mother_id", "col_date": "dum", 
            "col_lat": "lat", "col_lon": "lon"
        }
        return DEFAULT_DB_CONFIG, default_query

    def rodar_pipeline_backend(self):
        try:
            num_proc = int(self.num_processos_var.get())
            sincronizar = self.sincronizar_var.get()
            
            # Step 1: Mothers
            self.atualizar_progresso(0.1, "Starting Step 1: Mother Data...")
            
            pular_banco = False
            if os.path.exists(config.MAES_SAIDA) and not sincronizar:
                 self.atualizar_progresso(0.15, "Loading local data (Cache)...")
                 df_maes = pd.read_csv(config.MAES_SAIDA)
                 pular_banco = True
            
            if not pular_banco:
                self.atualizar_progresso(0.15, "Accessing Database...")
                
                db_cfg, query_cfg = self.get_active_db_config()
                
                df = carregar_prenatals_dinamico(db_cfg, query_cfg)
                
                self.atualizar_progresso(0.20, "Filtering data...")
                _, df_validos = aplicar_filtro_validade(df, DATA_CUTO, IDADE_MAX_DIAS)
                
                self.atualizar_progresso(0.25, "Preparing Geodata...")
                maes_gdf = preparar_maes_para_osm(df_validos)
                creches_gdf = carregar_creches()
                
                self.atualizar_progresso(0.30, "Loading Street Graph (OSM)...")
                _, poligono = carregar_municipio()
                G = carregar_grafo_osm(poligono, network_type="walk")
                
                self.atualizar_progresso(0.35, f"Calculating routes with {num_proc} processes...")
                df_maes = calcular_creche_mais_proxima(G, maes_gdf, creches_gdf, num_processos=num_proc)
                
                df_maes.to_csv(config.MAES_SAIDA, index=False)

            self.atualizar_progresso(0.50, "Step 1 Completed. Starting Maps...")

            # Step 2: Maps
            df_creches = pd.read_csv(config.CRECHES_CSV)
            
            self.atualizar_progresso(0.60, "Generating Cluster Map...")
            html_clusters = gerar_mapa_clusters(df_maes, df_creches)
            with open(config.MAPAS_CLUSTERS_DIR / "clusters.html", "w", encoding="utf-8") as f:
                f.write(html_clusters)
                
            self.atualizar_progresso(0.70, "Generating Thematic Map...")
            html_tematico = gerar_mapa_tematico(df_maes, config.BAIRROS_GEOJSON, df_creches)
            with open(config.MAPAS_TEMATICOS_DIR / "tematico.html", "w", encoding="utf-8") as f:
                f.write(html_tematico)

            # Step 3: Forecasts
            self.atualizar_progresso(0.80, "Generating Forecast Charts (ARIMA)...")
            gerar_previsoes_bairros()

            # Step 4: Report
            self.atualizar_progresso(0.90, "Assembling Final Report...")
            gerar_relatorio_final()

            self.atualizar_progresso(1.0, "Process Completed Successfully!")
            self.finalizar_sucesso()

        except Exception as e:
            self.finalizar_erro(str(e))

    def atualizar_progresso(self, valor, mensagem):
        self.progressbar.set(valor)
        self.label_status.configure(text=mensagem)
        print(f"[GUI] {mensagem}")

    def finalizar_sucesso(self):
        self.salvar_metadados()
        self.verificar_relatorio_existente()
        self.restaurar_botoes()
        messagebox.showinfo("Success", "Pipeline executed successfully!\nThe report is ready.")

    def finalizar_erro(self, erro_msg):
        self.label_status.configure(text="Execution error.")
        self.progressbar.configure(progress_color="red")
        self.restaurar_botoes()
        messagebox.showerror("Fatal Error", f"An error occurred during the pipeline:\n{erro_msg}")

    def restaurar_botoes(self):
        self.btn_executar.configure(state="normal", text="START PIPELINE")
        self.switch_sync.configure(state="normal")
        self.slider_cpu.configure(state="normal")
        self.btn_config.configure(state="normal")

if __name__ == "__main__":
    app = GeoCrecheApp()
    app.mainloop()