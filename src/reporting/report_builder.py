import os
from pathlib import Path
from src.utils.config import OUTPUT_DIR, GRAFICOS_PREV_DIR, MAPAS_CLUSTERS_DIR, MAPAS_TEMATICOS_DIR

OUTPUT_RELATORIO = OUTPUT_DIR / "relatorio_final.html"

def _carregar_arquivo_possiveis(dir_path, candidates):
    """
    Tries to load the first existing file from the `candidates` list
    inside dir_path. Returns (name_without_ext, content) or None if nothing is found.
    """
    if not dir_path.exists():
        return None
        
    for name in candidates:
        p = dir_path / name
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return name.replace(".html", "").replace("_", " ").title(), f.read()
    return None

def gerar_relatorio_final():
    print("[INFO] Building final report with enhanced interface...")

    # -------------------------------------------------------
    # 1) Load MAPS (clusters + thematic)
    # -------------------------------------------------------
    mapas = {}

    clusters_candidate = _carregar_arquivo_possiveis(
        MAPAS_CLUSTERS_DIR,
        ["clusters.html", "clusters/clusters.html", "clusters_index.html"]
    )
    if clusters_candidate:
        mapas["Clusters (Groupings)"] = clusters_candidate[1]
    else:
        print(f"[WARN] Cluster map not found in {MAPAS_CLUSTERS_DIR}")

    tematico_candidate = _carregar_arquivo_possiveis(
        MAPAS_TEMATICOS_DIR,
        ["tematico.html", "mapa_tematico.html", "tematico/index.html"]
    )
    if tematico_candidate:
        mapas["Thematic Map"] = tematico_candidate[1]
    else:
        print(f"[WARN] Thematic map not found in {MAPAS_TEMATICOS_DIR}")

    # -------------------------------------------------------
    # 2) Load PREDICTION CHARTS
    # -------------------------------------------------------
    graficos = {}
    if not GRAFICOS_PREV_DIR.exists():
        print(f"[WARN] Prediction charts directory not found: {GRAFICOS_PREV_DIR}")
    else:
        for g in sorted(os.listdir(GRAFICOS_PREV_DIR)):
            if g.endswith(".html"):
                nome_bairro = g.replace("_previsao.html", "").replace("_", " ")
                with open(GRAFICOS_PREV_DIR / g, "r", encoding="utf-8") as f:
                    graficos[nome_bairro] = f.read()

    # Reorder to ensure TOTAL is first
    if "TOTAL" in graficos:
        graficos = {"TOTAL": graficos["TOTAL"], **{k: v for k, v in graficos.items() if k != "TOTAL"}}

    # -------------------------------------------------------
    # HTML GENERATION (Bootstrap 5)
    # -------------------------------------------------------
    
    # HTML Header
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>GeoCrecheAI Report</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    
    <style>
        body { background-color: #f8f9fa; }
        .navbar { box-shadow: 0 2px 4px rgba(0,0,0,.1); }
        .card { box-shadow: 0 4px 6px rgba(0,0,0,.05); border: none; margin-bottom: 2rem; }
        .card-header { background-color: #fff; border-bottom: 1px solid #eee; font-weight: bold; font-size: 1.1rem; }
        
        /* --- MAP HEIGHT ADJUSTMENT --- */
        iframe { 
            width: 100% !important; 
            aspect-ratio: 16 / 9; /* Mantém a proporção de cinema, ideal para mapas */
            height: auto !important; 
            min-height: 500px;
            border-radius: 8px; 
            border: 1px solid #ddd; 
        }
        
        .plotly-graph-div, .plot-container { width: 100% !important; }
        
        /* Adjustment for small screens (Mobile) */
        @media (max-width: 768px) {
            iframe { height: 600px !important; }
        }
    </style>
</head>
<body>

    <nav class="navbar navbar-expand-lg navbar-dark bg-primary mb-4">
        <div class="container">
            <a class="navbar-brand" href="#"><i class="fas fa-map-marked-alt me-2"></i>GeoCrecheAI</a>
            <span class="navbar-text text-white opacity-75">Final Report</span>
        </div>
    </nav>

    <div class="container">
"""

    # -------------------------------------------------------
    # SECTION 1: MAPS
    # -------------------------------------------------------
    html += """
        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header d-flex justify-content-between align-items-center">
                        <span><i class="fas fa-globe-americas me-2 text-primary"></i>Spatial Visualization</span>
                    </div>
                    <div class="card-body">
    """

    if mapas:
        html += """
                        <label for="select-mapa" class="form-label text-muted small">Select visualization layer:</label>
                        <select id="select-mapa" class="form-select mb-3" onchange="selectMap()">
        """
        primeiro = True
        ids_mapas = [] # To store IDs and control JS display
        for i, nome_mapa in enumerate(mapas.keys()):
            selected = "selected" if primeiro else ""
            html += f'<option value="mapa-{i}" {selected}>{nome_mapa}</option>\n'
            ids_mapas.append(f"mapa-{i}")
            primeiro = False
        
        html += "</select>\n"

        # Map Content
        html += '<div id="mapa-container">\n'
        for i, (nome_mapa, conteudo) in enumerate(mapas.items()):
            display_style = "block" if i == 0 else "none"
            html += f'<div id="mapa-{i}" class="mapa-item" style="display:{display_style}; width:100%;">\n{conteudo}\n</div>\n'
        html += '</div>'

    else:
        html += '<div class="alert alert-warning">No maps found in output directories.</div>'

    html += """
                    </div> </div> </div> </div> """

    # -------------------------------------------------------
    # SECTION 2: CHARTS
    # -------------------------------------------------------
    html += """
        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <i class="fas fa-chart-line me-2 text-success"></i>Demand Forecasts (ARIMA)
                    </div>
                    <div class="card-body">
    """

    if graficos:
        html += """
                        <label for="select-bairro" class="form-label text-muted small">Select Neighborhood/Region:</label>
                        <select id="select-bairro" class="form-select mb-3" onchange="selectNeighborhood()">
        """
        primeiro = True
        ids_graficos = []
        for i, bairro in enumerate(graficos.keys()):
            selected = "selected" if primeiro else ""
            html += f'<option value="graf-{i}" {selected}>{bairro}</option>\n'
            ids_graficos.append(f"graf-{i}")
            primeiro = False
        
        html += "</select>\n"

        # Chart Content
        html += '<div id="grafico-container">\n'
        for i, (bairro, conteudo) in enumerate(graficos.items()):
            display_style = "block" if i == 0 else "none"
            # overflow-x style helps if the chart is too wide
            html += f'<div id="graf-{i}" class="grafico-item" style="display:{display_style}; overflow-x: hidden;">\n{conteudo}\n</div>\n'
        html += '</div>'
    
    else:
        html += '<div class="alert alert-secondary">No prediction charts generated.</div>'

    html += """
                    </div> </div> </div> </div> <footer class="text-center text-muted small py-4">
            Automatically generated by GeoCrecheAI &bull; <span id="data-geracao"></span>
        </footer>

    </div> <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        // Insert current date in footer
        document.getElementById('data-geracao').innerText = new Date().toLocaleDateString('en-US');

        function selectMap() {
            var selectedId = document.getElementById('select-mapa').value;
            var items = document.querySelectorAll('.mapa-item');
            
            items.forEach(function(el) {
                el.style.display = 'none';
            });
            
            var target = document.getElementById(selectedId);
            if(target) {
                target.style.display = 'block';
                // Trigger window resize event to force Plotly/Folium to redraw correctly inside new container
                window.dispatchEvent(new Event('resize'));
            }
        }

        function selectNeighborhood() {
            var selectedId = document.getElementById('select-bairro').value;
            var items = document.querySelectorAll('.grafico-item');
            
            items.forEach(function(el) {
                el.style.display = 'none';
            });
            
            var target = document.getElementById(selectedId);
            if(target) {
                target.style.display = 'block';
                window.dispatchEvent(new Event('resize'));
            }
        }
    </script>
</body>
</html>
"""

    # -------------------------------------------------------
    # Save
    # -------------------------------------------------------
    try:
        with open(OUTPUT_RELATORIO, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[OK] Final report (Modern Interface) generated at: {OUTPUT_RELATORIO}")
    except Exception as e:
        print(f"[ERROR] Failed to save report: {e}")

if __name__ == "__main__":
    gerar_relatorio_final()