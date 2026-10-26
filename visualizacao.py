"""Gera uma página HTML autocontida com o grafo de execução do pipeline.

As colunas são os níveis do DAG (uma tarefa só aparece depois de todas as suas
dependências) e a cor de cada nó mostra o resultado da última execução.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

LARGURA_NO, ALTURA_NO = 210, 56
PASSO_X, PASSO_Y = 270, 80
MARGEM = 24


def _layout(dag) -> tuple[dict[str, tuple[float, float]], float, float]:
    colunas: dict[int, list[str]] = {}
    for nome, nivel in dag.niveis().items():
        colunas.setdefault(nivel, []).append(nome)
    linhas_max = max(len(c) for c in colunas.values())

    y: dict[str, float] = {}
    pos: dict[str, tuple[float, float]] = {}
    for nivel in sorted(colunas):
        nos = colunas[nivel]
        if nivel > 0:
            # Baricentro: cada nó fica perto da média das dependências, o que reduz cruzamentos.
            nos.sort(key=lambda n: (sum(y[d] for d in dag.tarefas[n].depende_de) / len(dag.tarefas[n].depende_de), n))
        deslocamento = (linhas_max - len(nos)) / 2
        for i, n in enumerate(nos):
            y[n] = (i + deslocamento) * PASSO_Y + MARGEM
            pos[n] = (nivel * PASSO_X + MARGEM, y[n])
    largura = (len(colunas) - 1) * PASSO_X + LARGURA_NO + 2 * MARGEM
    altura = (linhas_max - 1) * PASSO_Y + ALTURA_NO + 2 * MARGEM
    return pos, largura, altura


def gerar_html(dag, resultados, destino: Path, id_execucao: str | None = None) -> Path:
    pos, largura, altura = _layout(dag)
    nos = []
    for i, nome in enumerate(dag.ordem):
        t = dag.tarefas[nome]
        r = (resultados or {}).get(nome)
        nos.append({
            "nome": nome, "ordem": i + 1, "tipo": t.tipo, "camada": t.camada, "descricao": t.descricao,
            "depende_de": t.depende_de, "sql": t.sql, "fonte": t.fonte,
            "x": pos[nome][0], "y": pos[nome][1],
            "status": r.status if r else "pendente",
            "duracao_ms": r.duracao_ms if r else None,
            "linhas": r.linhas if r else None,
            "tentativas": r.tentativas if r else None,
            "mensagem": r.mensagem if r else "",
        })
    dados = {
        "nos": nos, "largura": largura, "altura": altura, "largura_no": LARGURA_NO, "altura_no": ALTURA_NO,
        "id_execucao": id_execucao, "gerado_em": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "ondas": dag.ondas(),
    }
    json_seguro = json.dumps(dados, ensure_ascii=False).replace("</", "<\\/")
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(MODELO.replace("__DADOS__", json_seguro), encoding="utf-8")
    return destino


MODELO = r"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DAG de Vendas</title>
<style>
  :root {
    --bg: #f6f5f1; --surface: #ffffff; --surface-2: #f0efea; --text: #1f1e1b; --muted: #6a6862;
    --border: #dcd9d0; --edge: #a9a598; --accent: #2f5bd3;
    --ok: #237a4b; --ok-bg: #e5f2ea; --fail: #b93226; --fail-bg: #fbe7e4;
    --blocked: #85827a; --blocked-bg: #ecebe6; --pending-bg: #ffffff;
    --raw: #8d6e63; --staging: #1e78d6; --qualidade: #d79a00; --core: #7b52c7; --mart: #00897b; --publicacao: #cc2f6b;
  }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) {
      --bg: #161614; --surface: #201f1c; --surface-2: #2a2925; --text: #ecebe6; --muted: #a4a198;
      --border: #3a3833; --edge: #6d6a62; --accent: #7d9cf0;
      --ok: #5fc58c; --ok-bg: #18311f; --fail: #f07a6c; --fail-bg: #3a1c18;
      --blocked: #9c998f; --blocked-bg: #2a2925; --pending-bg: #201f1c;
    }
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--text);
         font: 14px/1.5 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
  header { padding: 24px 24px 8px; }
  h1 { margin: 0 0 4px; font-size: 22px; }
  h2 { font-size: 16px; margin: 0 0 12px; }
  .sub { color: var(--muted); margin: 0; }
  .chips { display: flex; flex-wrap: wrap; gap: 8px; margin: 14px 0 0; padding: 0; list-style: none; }
  .chip { background: var(--surface); border: 1px solid var(--border); border-radius: 999px; padding: 3px 12px; font-size: 13px; }
  .chip b { font-variant-numeric: tabular-nums; }
  .layout { display: grid; grid-template-columns: minmax(0, 1fr) 360px; gap: 16px; padding: 16px 24px 24px; }
  @media (max-width: 900px) { .layout { grid-template-columns: minmax(0, 1fr); padding: 16px; } header { padding: 16px 16px 4px; } }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 16px; min-width: 0; }
  .canvas { overflow: auto; }
  .legenda { display: flex; flex-wrap: wrap; gap: 14px; font-size: 12px; color: var(--muted); margin-bottom: 10px; }
  .legenda span { display: inline-flex; align-items: center; gap: 6px; }
  .sw { width: 12px; height: 12px; border-radius: 3px; display: inline-block; border: 1px solid var(--border); }
  svg { display: block; }
  .aresta { fill: none; stroke: var(--edge); stroke-width: 1.5; transition: opacity .15s; }
  .aresta.falha { stroke: var(--fail); stroke-dasharray: 5 4; }
  .seta { fill: var(--edge); }
  .no { cursor: pointer; transition: opacity .15s; outline: none; }
  .no .caixa { fill: var(--pending-bg); stroke: var(--border); stroke-width: 1.2; }
  .no.status-ok .caixa { fill: var(--ok-bg); stroke: var(--ok); }
  .no.status-falhou .caixa { fill: var(--fail-bg); stroke: var(--fail); stroke-width: 2; }
  .no.status-bloqueada .caixa { fill: var(--blocked-bg); stroke: var(--blocked); stroke-dasharray: 4 3; }
  .no.tipo-check .caixa { rx: 22; }
  .no.selecionado .caixa, .no:focus-visible .caixa { stroke: var(--accent); stroke-width: 2.5; }
  .no text { fill: var(--text); pointer-events: none; }
  .no .rotulo { font-size: 10.5px; fill: var(--muted); letter-spacing: .04em; text-transform: uppercase; }
  .no .nome { font-size: 13px; font-weight: 600; }
  .no .estado { font-size: 11px; fill: var(--muted); font-variant-numeric: tabular-nums; }
  .no.status-falhou .estado { fill: var(--fail); font-weight: 600; }
  .apagado { opacity: .18; }
  aside dl { display: grid; grid-template-columns: max-content 1fr; gap: 4px 12px; margin: 12px 0; font-size: 13px; }
  aside dt { color: var(--muted); }
  aside dd { margin: 0; overflow-wrap: anywhere; }
  .msg { padding: 8px 10px; border-radius: 8px; background: var(--fail-bg); color: var(--fail); font-size: 13px; overflow-wrap: anywhere; }
  pre { background: var(--surface-2); border-radius: 8px; padding: 12px; overflow: auto; font: 12px/1.45 ui-monospace, Consolas, monospace; max-height: 420px; }
  .vazio { color: var(--muted); }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--border); vertical-align: top; }
  th { color: var(--muted); font-weight: 500; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .tabela { overflow-x: auto; margin: 0 24px 32px; }
  @media (max-width: 900px) { .tabela { margin: 0 16px 24px; } }
  .st { font-weight: 600; }
  .st-ok { color: var(--ok); } .st-falhou { color: var(--fail); } .st-bloqueada, .st-pendente { color: var(--blocked); }
  tr { cursor: pointer; }
  tr:hover td { background: var(--surface-2); }
</style>
</head>
<body>
<header>
  <h1>DAG de execução: painel de vendas</h1>
  <p class="sub" id="sub"></p>
  <ul class="chips" id="chips"></ul>
</header>
<div class="layout">
  <section class="card canvas" aria-label="Grafo de execução">
    <div class="legenda" id="legenda"></div>
    <svg id="grafo" role="img" aria-label="Grafo direcionado acíclico das transformações"></svg>
  </section>
  <aside class="card" id="painel">
    <h2>Detalhes da etapa</h2>
    <p class="vazio">Clique em um nó para ver a responsabilidade da etapa, as dependências e o SQL. As tarefas que vêm antes e depois dela ficam destacadas.</p>
  </aside>
</div>
<section class="card tabela">
  <h2>Ordem de execução (topológica)</h2>
  <table>
    <thead><tr><th>#</th><th>Tarefa</th><th>Camada</th><th>Status</th><th class="num">Duração</th><th class="num">Linhas</th><th>Mensagem</th></tr></thead>
    <tbody id="linhas"></tbody>
  </table>
</section>
<script id="dados" type="application/json">__DADOS__</script>
<script>
const D = JSON.parse(document.getElementById('dados').textContent);
const NS = 'http://www.w3.org/2000/svg';
const W = D.largura_no, H = D.altura_no;
const porNome = Object.fromEntries(D.nos.map(n => [n.nome, n]));
const sucessores = Object.fromEntries(D.nos.map(n => [n.nome, []]));
D.nos.forEach(n => n.depende_de.forEach(d => sucessores[d].push(n.nome)));
const CAMADAS = ['raw', 'staging', 'qualidade', 'core', 'mart', 'publicacao'];

function el(tag, attrs, pai) {
  const e = document.createElementNS(NS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (pai) pai.appendChild(e);
  return e;
}
function h(tag, texto, classe) {
  const e = document.createElement(tag);
  if (texto != null) e.textContent = texto;
  if (classe) e.className = classe;
  return e;
}

// Cabeçalho e resumo
const cont = { ok: 0, falhou: 0, bloqueada: 0, pendente: 0 };
D.nos.forEach(n => cont[n.status]++);
const totalMs = D.nos.reduce((s, n) => s + (n.duracao_ms || 0), 0);
document.getElementById('sub').textContent = D.id_execucao
  ? `Execução ${D.id_execucao} · gerado em ${D.gerado_em}`
  : `Grafo validado, ainda sem execução · gerado em ${D.gerado_em}`;
const chips = document.getElementById('chips');
[[`${D.nos.length}`, 'tarefas'], [`${D.nos.reduce((s, n) => s + n.depende_de.length, 0)}`, 'dependências'],
 [`${D.ondas.length}`, 'ondas'], ...(D.id_execucao ? [[cont.ok, 'ok'], [cont.falhou, 'falharam'], [cont.bloqueada, 'bloqueadas'], [`${totalMs} ms`, 'no total']] : [])]
  .forEach(([v, r]) => { const li = h('li', null, 'chip'); li.append(h('b', String(v)), ' ' + r); chips.append(li); });

const legenda = document.getElementById('legenda');
CAMADAS.forEach(c => { const s = h('span'); const sw = h('i', null, 'sw'); sw.style.background = `var(--${c})`; s.append(sw, c); legenda.append(s); });
[['ok', 'var(--ok-bg)'], ['falhou', 'var(--fail-bg)'], ['bloqueada', 'var(--blocked-bg)']].forEach(([t, cor]) => {
  const s = h('span'); const sw = h('i', null, 'sw'); sw.style.background = cor; s.append(sw, t); legenda.append(s);
});
const sCheck = h('span', 'nó arredondado = check de qualidade'); legenda.append(sCheck);

// Grafo
const svg = document.getElementById('grafo');
svg.setAttribute('viewBox', `0 0 ${D.largura} ${D.altura}`);
svg.setAttribute('width', D.largura);
svg.setAttribute('height', D.altura);
const marcador = el('marker', { id: 'seta', viewBox: '0 0 10 10', refX: '9', refY: '5', markerWidth: '7', markerHeight: '7', orient: 'auto' }, el('defs', {}, svg));
el('path', { d: 'M0,0 L10,5 L0,10 z', class: 'seta' }, marcador);
const gArestas = el('g', {}, svg), gNos = el('g', {}, svg);

const arestas = [];
D.nos.forEach(n => n.depende_de.forEach(d => {
  const a = porNome[d];
  const x1 = a.x + W, y1 = a.y + H / 2, x2 = n.x - 1, y2 = n.y + H / 2, mx = (x1 + x2) / 2;
  const falha = a.status === 'falhou' || a.status === 'bloqueada';
  const p = el('path', { d: `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`, class: 'aresta' + (falha ? ' falha' : ''), 'marker-end': 'url(#seta)' }, gArestas);
  arestas.push({ de: d, para: n.nome, el: p });
}));

D.nos.forEach(n => {
  const g = el('g', { class: `no status-${n.status} tipo-${n.tipo}`, transform: `translate(${n.x},${n.y})`, tabindex: '0', role: 'button', 'aria-label': `${n.nome}: ${n.status}` }, gNos);
  el('rect', { class: 'caixa', width: W, height: H, rx: n.tipo === 'check' ? 22 : 8 }, g);
  el('rect', { x: n.tipo === 'check' ? 10 : 0, y: 8, width: 5, height: H - 16, rx: 2.5, fill: `var(--${n.camada})` }, g);
  el('text', { x: 22, y: 21, class: 'rotulo' }, g).textContent = `${n.ordem}. ${n.tipo} · ${n.camada}`;
  el('text', { x: 22, y: 41, class: 'nome' }, g).textContent = n.nome;
  if (n.status !== 'pendente') {
    el('text', { x: W - 12, y: 21, class: 'estado', 'text-anchor': 'end' }, g).textContent =
      n.status === 'ok' ? `${n.duracao_ms} ms` : n.status;
  }
  g.addEventListener('click', () => selecionar(n.nome));
  g.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); selecionar(n.nome); } });
  n.el = g;
});

function fecho(nome, vizinhos, acc = new Set()) {
  vizinhos(nome).forEach(v => { if (!acc.has(v)) { acc.add(v); fecho(v, vizinhos, acc); } });
  return acc;
}

let atual = null;
function selecionar(nome) {
  atual = atual === nome ? null : nome;
  const acima = atual ? fecho(atual, x => porNome[x].depende_de) : new Set();
  const abaixo = atual ? fecho(atual, x => sucessores[x]) : new Set();
  const visiveis = new Set([atual, ...acima, ...abaixo]);
  D.nos.forEach(n => {
    n.el.classList.toggle('apagado', !!atual && !visiveis.has(n.nome));
    n.el.classList.toggle('selecionado', n.nome === atual);
  });
  arestas.forEach(a => a.el.classList.toggle('apagado', !!atual && !(visiveis.has(a.de) && visiveis.has(a.para))));
  if (atual) mostrar(porNome[atual], acima, abaixo);
}

function mostrar(n, acima, abaixo) {
  const painel = document.getElementById('painel');
  painel.replaceChildren(h('h2', n.nome), h('p', n.descricao));
  if (n.mensagem) painel.append(h('div', n.mensagem, 'msg'));
  const dl = h('dl');
  const campos = [
    ['Tipo', n.tipo], ['Camada', n.camada], ['Posição', `${n.ordem}º na ordem topológica`],
    ['Fonte', n.fonte], ['Depende de', n.depende_de.join(', ') || 'nada (é uma fonte)'],
    ['Alimenta', sucessores[n.nome].join(', ') || 'nada (é uma saída)'],
    ['Impacto', `${abaixo.size} tarefa(s) a jusante dependem dela`],
    ['Status', n.status], ['Duração', n.duracao_ms != null ? `${n.duracao_ms} ms` : null],
    ['Linhas', n.linhas], ['Tentativas', n.tentativas],
  ];
  campos.forEach(([k, v]) => { if (v != null && v !== '') dl.append(h('dt', k), h('dd', String(v))); });
  painel.append(dl, h('pre', n.sql));
}

// Tabela da ordem de execução
const tbody = document.getElementById('linhas');
D.nos.forEach(n => {
  const tr = h('tr');
  tr.append(h('td', n.ordem, 'num'), h('td', n.nome), h('td', n.camada), h('td', n.status, `st st-${n.status}`),
    h('td', n.duracao_ms != null ? `${n.duracao_ms} ms` : '', 'num'), h('td', n.linhas ?? '', 'num'), h('td', n.mensagem));
  tr.addEventListener('click', () => { if (atual !== n.nome) selecionar(n.nome); n.el.scrollIntoView({ block: 'nearest', inline: 'center', behavior: 'smooth' }); });
  tbody.append(tr);
});
</script>
</body>
</html>
"""
