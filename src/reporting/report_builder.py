import os
from src.utils.config import OUTPUT_DIR, GRAFICOS_PREV_DIR, MAPAS_CLUSTERS_DIR, MAPAS_TEMATICOS_DIR

OUTPUT_RELATORIO = OUTPUT_DIR / "relatorio_final.html"


def _carregar_arquivo_possiveis(dir_path, candidates):
    """
    Tenta carregar o primeiro arquivo existente da lista `candidates`
    dentro de dir_path. Retorna (nome_sem_ext, conteudo) ou None se nada achar.
    """
    for name in candidates:
        p = dir_path / name
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                return name.replace(".html", ""), f.read()
    return None


def gerar_relatorio_final():
    print("[INFO] Montando relatório final...")

    # -------------------------------------------------------
    # 1) Carregar MAPAS (clusters + temático) - caminhos corrigidos
    # -------------------------------------------------------
    mapas = {}

    # possíveis nomes/locais que podem ter sido usados
    # clusters: pode estar em MAPAS_CLUSTERS_DIR/clusters.html ou MAPAS_CLUSTERS_DIR/clusters/clusters.html (caso aninhado)
    clusters_candidate = _carregar_arquivo_possiveis(
        MAPAS_CLUSTERS_DIR,
        ["clusters.html", "clusters/clusters.html", "clusters_index.html"]
    )
    if clusters_candidate:
        mapas[clusters_candidate[0]] = clusters_candidate[1]
    else:
        print(f"[WARN] Mapa de clusters não encontrado em {MAPAS_CLUSTERS_DIR}")

    # temático: pode estar em MAPAS_TEMATICOS_DIR/tematico.html ou mapa_tematico.html
    tematico_candidate = _carregar_arquivo_possiveis(
        MAPAS_TEMATICOS_DIR,
        ["tematico.html", "mapa_tematico.html", "tematico/index.html"]
    )
    if tematico_candidate:
        mapas[tematico_candidate[0]] = tematico_candidate[1]
    else:
        print(f"[WARN] Mapa temático não encontrado em {MAPAS_TEMATICOS_DIR}")

    # -------------------------------------------------------
    # 2) Carregar GRÁFICOS DE PREVISÃO
    # -------------------------------------------------------
    graficos = {}
    if not GRAFICOS_PREV_DIR.exists():
        print(f"[WARN] Diretório de gráficos de previsão não encontrado: {GRAFICOS_PREV_DIR}")
    else:
        for g in sorted(os.listdir(GRAFICOS_PREV_DIR)):
            if g.endswith(".html"):
                nome_bairro = g.replace("_previsao.html", "")
                with open(GRAFICOS_PREV_DIR / g, "r", encoding="utf-8") as f:
                    graficos[nome_bairro] = f.read()

    # -------------------------------------------------------
    # HTML principal
    # -------------------------------------------------------
    html = """
    <html>
    <head>
    <title>Relatório GeoCrecheAI</title>
    <meta charset="UTF-8">

    <style>
    body {
        font-family: Arial, sans-serif;
        margin: 20px;
    }
    #grafico-container > div, #mapa-container > div {
        display: none;
    }
    .plot-container, .plotly-graph-div {
        width: 900px !important;
        height: 550px !important;
        margin: auto;
    }
    </style>

    <script>
    function selecionarMapa() {
        var m = document.getElementById('select-mapa').value;
        var blocos = document.querySelectorAll('#mapa-container > div');
        blocos.forEach(b => b.style.display = 'none');
        if (m) document.getElementById('mapa-' + m).style.display = 'block';
    }

    function selecionarBairro() {
        var bairro = document.getElementById('select-bairro').value;
        var blocos = document.querySelectorAll('#grafico-container > div');
        blocos.forEach(b => b.style.display = 'none');
        if (bairro) document.getElementById('graf-' + bairro).style.display = 'block';
    }

    window.onload = function() {
        selecionarMapa();
        selecionarBairro();
    };
    </script>
    </head>
    <body>

    <h1>Relatório GeoCrecheAI</h1>
    """

    # -------------------------------------------------------
    # MAPAS (clusters + temático)
    # -------------------------------------------------------
    html += """
    <h2>Mapas Principais</h2>
    <label><b>Selecione o mapa:</b></label>
    <select id="select-mapa" onchange="selecionarMapa()">
    """

    if mapas:
        primeiro = True
        for nome_mapa in mapas.keys():
            if primeiro:
                html += f'<option value="{nome_mapa}" selected>{nome_mapa}</option>\n'
                primeiro = False
            else:
                html += f'<option value="{nome_mapa}">{nome_mapa}</option>\n'
    else:
        html += '<option value="">--Nenhum mapa disponível--</option>\n'

    html += """
    </select>

    <div id="mapa-container">
    """

    primeiro = True
    for nome_mapa, conteudo in mapas.items():
        display = "block" if primeiro else "none"
        html += f'<div id="mapa-{nome_mapa}" style="display:{display}">\n{conteudo}\n</div>\n'
        primeiro = False

    html += "</div><hr>"

    # -------------------------------------------------------
    # PREVISÕES POR BAIRRO
    # -------------------------------------------------------
    html += """
    <h2>Previsões por Bairro</h2>
    <label><b>Selecione um bairro:</b></label>
    <select id="select-bairro" onchange="selecionarBairro()">
    """

    if graficos:
        primeiro = True
        for bairro in graficos.keys():
            if primeiro:
                html += f'<option value="{bairro}" selected>{bairro}</option>\n'
                primeiro = False
            else:
                html += f'<option value="{bairro}">{bairro}</option>\n'
    else:
        html += '<option value="">--Nenhum gráfico disponível--</option>\n'

    html += """
    </select>
    <hr><br>

    <div id="grafico-container">
    """

    primeiro = True
    for bairro, conteudo in graficos.items():
        display = "block" if primeiro else "none"
        html += f'<div id="graf-{bairro}" style="display:{display}">\n{conteudo}\n</div>\n'
        primeiro = False

    html += """
    </div>

    </body>
    </html>
    """

    # -------------------------------------------------------
    # Salvar
    # -------------------------------------------------------
    with open(OUTPUT_RELATORIO, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] Relatório final gerado em: {OUTPUT_RELATORIO}")


if __name__ == "__main__":
    gerar_relatorio_final()
