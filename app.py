from flask import Flask, render_template, request, redirect, url_for, send_file
from datetime import datetime, date, time
import io
import os
import re

import psycopg2
from psycopg2.extras import DictCursor

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
)
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import cm

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")


# =========================
# HELPERS DE BANCO
# =========================

def adapt_sql(sql: str) -> str:
    """
    Converte SQL estilo SQLite para PostgreSQL.
    """
    # strftime -> TO_CHAR
    sql = sql.replace("strftime('%Y-%m', data_agendamento)", "TO_CHAR(data_agendamento, 'YYYY-MM')")
    sql = sql.replace("strftime('%Y-%m', p.data_agendamento)", "TO_CHAR(p.data_agendamento, 'YYYY-MM')")
    sql = sql.replace("strftime('%Y-%m', co.data)", "TO_CHAR(co.data, 'YYYY-MM')")
    sql = sql.replace("strftime('%Y-%m', data)", "TO_CHAR(data, 'YYYY-MM')")
    sql = sql.replace("strftime('%Y', data)", "TO_CHAR(data, 'YYYY')")
    sql = sql.replace("strftime('%Y', data_agendamento)", "TO_CHAR(data_agendamento, 'YYYY')")
    sql = sql.replace("strftime('%Y-%m', p.data_agendamento)=?", "TO_CHAR(p.data_agendamento, 'YYYY-MM')=%s")

    # date(?) -> CAST(%s AS DATE)
    sql = re.sub(r"date\(\s*\?\s*\)", "CAST(%s AS DATE)", sql)

    # placeholders ? -> %s
    sql = sql.replace("?", "%s")

    # placeholders quebrados com % -> %s
    sql = re.sub(r"(?<!%)%(?!s)", "%s", sql)

    return sql


class PGCursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    def execute(self, sql, params=None):
        sql = adapt_sql(sql)
        self.cursor.execute(sql, params or ())
        return self  # permite c.execute(...).fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def __getattr__(self, item):
        return getattr(self.cursor, item)


class PGConnectionWrapper:
    def __init__(self, conn):
        self.conn = conn

    def cursor(self):
        return PGCursorWrapper(self.conn.cursor())

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def __getattr__(self, item):
        return getattr(self.conn, item)


def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não foi definida.")
    conn = psycopg2.connect(
        DATABASE_URL,
        cursor_factory=DictCursor,
        sslmode="require"
    )
    return PGConnectionWrapper(conn)


# =========================
# DATABASE
# =========================

def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS contratos (
        id SERIAL PRIMARY KEY,
        numero_cnt TEXT,
        nome_contrato TEXT,
        contato TEXT,
        telefone TEXT,
        email TEXT,
        endereco TEXT,
        preventivas_mes INTEGER DEFAULT 1,
        ativo INTEGER DEFAULT 1
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS preventivas (
        id SERIAL PRIMARY KEY,
        contrato_id INTEGER,
        data_agendamento DATE,
        status TEXT DEFAULT 'Pendente'
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS corretivas (
        id SERIAL PRIMARY KEY,
        contrato_id INTEGER,
        data DATE,
        ocorrencia TEXT,
        contato TEXT,
        status TEXT DEFAULT 'Pendente'
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS visitas_tecnicas (
        id SERIAL PRIMARY KEY,
        local TEXT,
        contato TEXT,
        data DATE,
        hora TIME,
        status TEXT DEFAULT 'Pendente',
        observacao TEXT
    )
    """)

    conn.commit()
    conn.close()


# inicializa tabelas
init_db()


# =========================
# ESTILOS / HELPERS PDF
# =========================

styles = getSampleStyleSheet()

# =========================
# HOME
# =========================

@app.route("/")
def home():
    return redirect(url_for("dashboard"))


@app.route("/relatorio")
def relatorio():
    # input type="month" (YYYY-MM)
    mes_padrao = datetime.now().strftime("%Y-%m")
    return render_template("relatorio.html", mes_padrao=mes_padrao)


# =========================
# CONTRATOS
# =========================

@app.route("/contratos", methods=["GET", "POST"])
def contratos():
    conn = get_db()
    c = conn.cursor()

    if request.method == "POST":
        c.execute("""
        INSERT INTO contratos 
        (numero_cnt, nome_contrato, contato, telefone, email, endereco, preventivas_mes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            request.form["numero_cnt"],
            request.form["nome_contrato"],
            request.form["contato"],
            request.form["telefone"],
            request.form["email"],
            request.form["endereco"],
            request.form["preventivas_mes"]
        ))
        conn.commit()
        conn.close()
        return redirect(url_for("contratos"))

    contratos = c.execute("SELECT * FROM contratos WHERE ativo=1").fetchall()
    conn.close()
    return render_template("contratos.html", contratos=contratos)

@app.route("/editar_contrato/<int:id>", methods=["POST"])
def editar_contrato(id):
    conn = get_db()
    c = conn.cursor()

    numero_cnt = request.form.get("numero_cnt", "").strip()
    nome_contrato = request.form.get("nome_contrato", "").strip()
    contato = request.form.get("contato", "").strip()
    telefone = request.form.get("telefone", "").strip()
    email = request.form.get("email", "").strip()
    endereco = request.form.get("endereco", "").strip()
    preventivas_mes = request.form.get("preventivas_mes", "1").strip()

    # garante inteiro válido
    try:
        preventivas_mes = int(preventivas_mes)
    except ValueError:
        preventivas_mes = 1

    c.execute("""
        UPDATE contratos
        SET numero_cnt=%, nome_contrato=%, contato=%, telefone=%, email=%, endereco=%, preventivas_mes=%
        WHERE id=?
    """, (numero_cnt, nome_contrato, contato, telefone, email, endereco, preventivas_mes, id))

    conn.commit()
    conn.close()
    return redirect(url_for("contratos"))


@app.route("/excluir_contrato/<int:id>")
def excluir_contrato(id):
    # Melhor prática: não apagar, só desativar
    conn = get_db()
    c = conn.cursor()
    c.execute("UPDATE contratos SET ativo=0 WHERE id=%", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("contratos"))


# =========================
# PREVENTIVA
# =========================

@app.route("/preventiva", methods=["GET", "POST"])
def preventiva():
    conn = get_db()
    c = conn.cursor()

    mes_atual = datetime.now().strftime("%Y-%m")

    if request.method == "POST":
        contrato_id = request.form["contrato_id"]
        data_agendamento = request.form["data_agendamento"]
        mes_data = data_agendamento[:7]

        c.execute("""
            SELECT preventivas_mes
            FROM contratos
            WHERE id=%s
        """, (contrato_id,))
        row = c.fetchone()
        limite = int(row["preventivas_mes"]) if row else 0

        c.execute("""
            SELECT COUNT(*) AS total
            FROM preventivas
            WHERE contrato_id=%s
              AND TO_CHAR(data_agendamento, 'YYYY-MM')=%s
        """, (contrato_id, mes_data))
        agendadas = int(c.fetchone()["total"])

        if agendadas < limite:
            c.execute("""
                INSERT INTO preventivas (contrato_id, data_agendamento)
                VALUES (%s, %s)
            """, (contrato_id, data_agendamento))
            conn.commit()

        conn.close()
        return redirect(url_for("preventiva"))

    c.execute("""
        SELECT *
        FROM contratos
        WHERE ativo=1
        ORDER BY nome_contrato ASC
    """)
    contratos = c.fetchall()

    c.execute("""
        SELECT p.*, c.nome_contrato
        FROM preventivas p
        LEFT JOIN contratos c ON p.contrato_id = c.id
        ORDER BY p.data_agendamento DESC
    """)
    preventivas = c.fetchall()

    c.execute("""
        SELECT COALESCE(SUM(preventivas_mes), 0) AS total_previstas
        FROM contratos
        WHERE ativo=1
    """)
    total_previstas = int(c.fetchone()["total_previstas"])

    c.execute("""
        SELECT COUNT(*) AS total_concluidas
        FROM preventivas
        WHERE status='Concluída'
          AND TO_CHAR(data_agendamento, 'YYYY-MM')=%s
    """, (mes_atual,))
    concluidos = int(c.fetchone()["total_concluidas"])

    pendentes = max(total_previstas - concluidos, 0)

    conn.close()

    return render_template(
        "preventiva.html",
        contratos=contratos,
        preventivas=preventivas,
        concluidos=concluidos,
        pendentes=pendentes,
        mes_atual=mes_atual
    )

@app.route("/concluir_preventiva/<int:id>")
def concluir_preventiva(id):
    conn = get_db()
    c = conn.cursor()
    c.execute("UPDATE preventivas SET status='Concluída' WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("preventiva"))


@app.route("/excluir_preventiva/<int:id>")
def excluir_preventiva(id):
    conn = get_db()
    c = conn.cursor()
    c.execute("DELETE FROM preventivas WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("preventiva"))

@app.route("/editar_preventiva/<int:id>", methods=["POST"])
def editar_preventiva(id):
    conn = get_db()
    c = conn.cursor()

    nova_data = request.form.get("nova_data")

    if nova_data:
        c.execute("""
            UPDATE preventivas
            SET data_agendamento=?
            WHERE id=?
        """, (nova_data, id))

        conn.commit()

    conn.close()

    return redirect(url_for("preventiva"))

@app.route("/editar_visita/<int:id>", methods=["POST"])
def editar_visita(id):
    conn = get_db()
    c = conn.cursor()

    nova_data = request.form.get("nova_data")
    nova_hora = request.form.get("nova_hora")
    nova_observacao = request.form.get("nova_observacao")

    c.execute("""
        UPDATE visitas_tecnicas
        SET data=?, hora=?, observacao=?
        WHERE id=?
    """, (nova_data, nova_hora, nova_observacao, id))

    conn.commit()
    conn.close()
    return redirect(url_for("visita_tecnica"))


@app.route("/excluir_visita/<int:id>")
def excluir_visita(id):
    conn = get_db()
    c = conn.cursor()
    c.execute("DELETE FROM visitas_tecnicas WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("visita_tecnica"))


# =========================
# CORRETIVA
# =========================

@app.route("/corretiva", methods=["GET", "POST"])
def corretiva():
    conn = get_db()
    c = conn.cursor()

    if request.method == "POST":
        c.execute("""
        INSERT INTO corretivas (contrato_id, data, contato, status, ocorrencia)
        VALUES (?, ?, ?, ?, ?)
        """, (
            request.form.get("contrato_id"),
            request.form["data"],
            request.form.get("contato"),
            request.form.get("status", "Pendente"),
            request.form["ocorrencia"]
        ))
        conn.commit()
        conn.close()
        return redirect(url_for("corretiva"))

    contratos = c.execute("SELECT * FROM contratos WHERE ativo=1").fetchall()

    corretivas = c.execute("""
        SELECT co.*, c.nome_contrato
        FROM corretivas co
        LEFT JOIN contratos c ON co.contrato_id = c.id
        ORDER BY co.data DESC
    """).fetchall()

    conn.close()
    return render_template("corretiva.html",
                           contratos=contratos,
                           corretivas=corretivas)


# =========================
# VISITA TÉCNICA
# =========================

@app.route("/visita_tecnica", methods=["GET", "POST"])
def visita_tecnica():
    conn = get_db()
    c = conn.cursor()

    if request.method == "POST":
        c.execute("""
        INSERT INTO visitas_tecnicas (local, contato, data, hora, observacao)
        VALUES (?, ?, ?, ?, ?)
        """, (
            request.form["local"],
            request.form["contato"],
            request.form["data"],
            request.form["hora"],
            request.form["observacao"]
        ))
        conn.commit()
        conn.close()
        return redirect(url_for("visita_tecnica"))

    visitas = c.execute("""
        SELECT *
        FROM visitas_tecnicas
        ORDER BY data DESC, hora ASC
    """).fetchall()

    conn.close()
    return render_template("visita_tecnica.html", visitas=visitas)


@app.route("/concluir_visita/<int:id>")
def concluir_visita(id):
    conn = get_db()
    c = conn.cursor()
    c.execute("UPDATE visitas_tecnicas SET status=? WHERE id=?", ("Concluída", id))
    conn.commit()
    conn.close()
    return redirect(url_for("visita_tecnica"))

@app.route("/editar_corretiva/<int:id>", methods=["POST"])
def editar_corretiva(id):
    conn = get_db()
    c = conn.cursor()

    nova_data = request.form.get("nova_data")
    novo_contato = request.form.get("novo_contato")
    novo_status = request.form.get("novo_status")
    nova_ocorrencia = request.form.get("nova_ocorrencia")

    c.execute("""
        UPDATE corretivas
        SET data=?, contato=?, status=?, ocorrencia=?
        WHERE id=?
    """, (nova_data, novo_contato, novo_status, nova_ocorrencia, id))

    conn.commit()
    conn.close()
    return redirect(url_for("corretiva"))


@app.route("/excluir_corretiva/<int:id>")
def excluir_corretiva(id):
    conn = get_db()
    c = conn.cursor()
    c.execute("DELETE FROM corretivas WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("corretiva"))


# =========================
# DASHBOARD
# =========================

@app.route("/dashboard")
def dashboard():
    conn = get_db()
    c = conn.cursor()

    hoje = datetime.now().strftime("%Y-%m-%d")
    mes_atual = datetime.now().strftime("%Y-%m")
    ano_atual = datetime.now().strftime("%Y")

    # VISITAS PENDENTES (MÊS ATUAL) - robusto
    visitas_pendentes_mes = c.execute("""
        SELECT COUNT(*)
        FROM visitas_tecnicas
        WHERE TRIM(LOWER(status)) = 'pendente'
          AND strftime('%Y-%m', data) = ?
    """, (mes_atual,)).fetchone()[0]

    # =========================
    # KPIs PREVENTIVAS (MÊS)
    # Total previsto = soma preventivas_mes dos contratos ativos
    # Concluídas = qtd preventivas concluídas no mês
    # Pendentes = previsto - concluídas (inclui "não agendadas")
    # =========================
    total_previstas = c.execute("""
        SELECT COALESCE(SUM(preventivas_mes), 0)
        FROM contratos
        WHERE ativo=1
    """).fetchone()[0]

    concluidas_mes = c.execute("""
        SELECT COUNT(*)
        FROM preventivas
        WHERE status='Concluída'
        AND strftime('%Y-%m', data_agendamento)=?
    """, (mes_atual,)).fetchone()[0]

    total_agendadas_mes = c.execute("""
        SELECT COUNT(*)
        FROM preventivas
        WHERE strftime('%Y-%m', data_agendamento)=?
    """, (mes_atual,)).fetchone()[0]

    pendentes_mes = max(int(total_previstas) - int(concluidas_mes), 0)
    sem_agendamento = max(int(total_previstas) - int(total_agendadas_mes), 0)

    percent_conclusao = 0
    if int(total_previstas) > 0:
        percent_conclusao = round((int(concluidas_mes) / int(total_previstas)) * 100)

    # atrasadas = preventivas pendentes com data < hoje (somente as agendadas)
    atrasadas = c.execute("""
        SELECT COUNT(*)
        FROM preventivas
        WHERE status!='Concluída'
        AND strftime('%Y-%m', data_agendamento)=?
        AND date(data_agendamento) < date(?)
    """, (mes_atual, hoje)).fetchone()[0]

    # =========================
    # CORRETIVAS (ANO)
    # =========================
    corretivas_ano = c.execute("""
        SELECT COUNT(*)
        FROM corretivas
        WHERE strftime('%Y', data)=?
    """, (ano_atual,)).fetchone()[0]

    corretivas_bo_ano = c.execute("""
        SELECT COUNT(*)
        FROM corretivas
        WHERE strftime('%Y', data)=?
        AND status='BO'
    """, (ano_atual,)).fetchone()[0]

    corretivas_pendentes = c.execute("""
        SELECT COUNT(*)
        FROM corretivas
        WHERE strftime('%Y', data)=?
        AND status='Pendente'
    """, (ano_atual,)).fetchone()[0]

    # =========================
    # VISITAS (ANO)
    # =========================
    visitas_concluidas_ano = c.execute("""
        SELECT COUNT(*)
        FROM visitas_tecnicas
        WHERE strftime('%Y', data)=?
        AND status='Concluída'
    """, (ano_atual,)).fetchone()[0]

    visitas_pendentes_ano = c.execute("""
        SELECT COUNT(*)
        FROM visitas_tecnicas
        WHERE strftime('%Y', data)=?
        AND status='Pendente'
    """, (ano_atual,)).fetchone()[0]

    # VISITAS PENDENTES (MÊS)
    visitas_pendentes_mes = c.execute("""
        SELECT COUNT(*)
        FROM visitas_tecnicas
        WHERE status='Pendente'
        AND strftime('%Y-%m', data)=?
    """, (mes_atual,)).fetchone()[0]

    visitas_pendentes_lista = c.execute("""
        SELECT *
        FROM visitas_tecnicas
        WHERE status='Pendente'
        ORDER BY data ASC, hora ASC
        LIMIT 8
    """).fetchall()

    # =========================
    # LISTAS ÚTEIS
    # Próximas preventivas (pendentes) - próximas 10
    # =========================
    proximas_preventivas = c.execute("""
        SELECT p.id, p.data_agendamento, p.status, ct.nome_contrato
        FROM preventivas p
        LEFT JOIN contratos ct ON ct.id = p.contrato_id
        WHERE p.status != 'Concluída'
        AND date(p.data_agendamento) >= date(?)
        ORDER BY date(p.data_agendamento) ASC
        LIMIT 10
    """, (hoje,)).fetchall()

    # Últimas corretivas BO/Pendente - últimas 10
    ultimas_corretivas_alerta = c.execute("""
        SELECT co.id, co.data, co.status, co.ocorrencia, ct.nome_contrato
        FROM corretivas co
        LEFT JOIN contratos ct ON ct.id = co.contrato_id
        WHERE co.status IN ('BO', 'Pendente')
        ORDER BY date(co.data) DESC
        LIMIT 10
    """).fetchall()

    # =========================
    # GRÁFICO: Preventivas concluídas últimos 6 meses
    # =========================
    def month_back(n):
        # retorna "YYYY-MM" de n meses atrás (inclui mês atual com n=0)
        dt = datetime.now()
        y = dt.year
        m = dt.month - n
        while m <= 0:
            m += 12
            y -= 1
        return f"{y:04d}-{m:02d}"

    meses_6 = [month_back(i) for i in range(5, -1, -1)]  # do mais antigo -> atual

    # nomes em PT-BR (curto)
    nomes_mes = {
        "01": "Jan", "02": "Fev", "03": "Mar", "04": "Abr", "05": "Mai", "06": "Jun",
        "07": "Jul", "08": "Ago", "09": "Set", "10": "Out", "11": "Nov", "12": "Dez"
    }
    labels_6 = [f"{nomes_mes[m[-2:]]}/{m[:4][-2:]}" for m in meses_6]

    concluidas_6 = []
    for m in meses_6:
        qtd = c.execute("""
            SELECT COUNT(*)
            FROM preventivas
            WHERE status='Concluída'
            AND strftime('%Y-%m', data_agendamento)=?
        """, (m,)).fetchone()[0]
        concluidas_6.append(int(qtd))

    # =========================
    # GRÁFICO: Visitas por mês (ano atual) - pendentes x concluídas
    # =========================
    labels_12 = [nomes_mes[f"{i:02d}"] for i in range(1, 13)]
    visitas_conc_12 = []
    visitas_pend_12 = []
    for i in range(1, 13):
        mes = f"{ano_atual}-{i:02d}"
        vc = c.execute("""
            SELECT COUNT(*) FROM visitas_tecnicas
            WHERE status='Concluída' AND strftime('%Y-%m', data)=?
        """, (mes,)).fetchone()[0]
        vp = c.execute("""
            SELECT COUNT(*) FROM visitas_tecnicas
            WHERE status='Pendente' AND strftime('%Y-%m', data)=?
        """, (mes,)).fetchone()[0]
        visitas_conc_12.append(int(vc))
        visitas_pend_12.append(int(vp))

    conn.close()

    return render_template(
        "dashboard.html",
        # KPIs
        mes_atual=mes_atual,
        total_previstas=int(total_previstas),
        concluidas_mes=int(concluidas_mes),
        pendentes_mes=int(pendentes_mes),
        percent_conclusao=int(percent_conclusao),
        atrasadas=int(atrasadas),
        sem_agendamento=int(sem_agendamento),

        corretivas_ano=int(corretivas_ano),
        corretivas_bo_ano=int(corretivas_bo_ano),
        corretivas_pendentes=int(corretivas_pendentes),

        visitas_concluidas_ano=int(visitas_concluidas_ano),
        visitas_pendentes_ano=int(visitas_pendentes_ano),
        visitas_pendentes_mes=visitas_pendentes_mes,
        visitas_pendentes_lista=visitas_pendentes_lista,

        # Listas
        proximas_preventivas=proximas_preventivas,
        ultimas_corretivas_alerta=ultimas_corretivas_alerta,

        # Charts
        labels_6=labels_6,
        concluidas_6=concluidas_6,
        labels_12=labels_12,
        visitas_conc_12=visitas_conc_12,
        visitas_pend_12=visitas_pend_12
    )


import os
import matplotlib.pyplot as plt
import numpy as np

def _save_donut_chart(labels, values, colors, title, outpath):
    import os
    import numpy as np
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    vals = []
    for v in values:
        try:
            v = int(v)
        except Exception:
            v = 0
        vals.append(max(0, v))

    total = sum(vals)

    fig = plt.figure(figsize=(6.2, 4.2), dpi=160)
    ax = plt.gca()

    ax.set_title(title, fontsize=11, fontweight="bold", pad=10)

    if total <= 0:
        ax.text(0.5, 0.52, "Sem dados", ha="center", va="center", fontsize=14, fontweight="bold")
        ax.text(0.5, 0.40, "0", ha="center", va="center", fontsize=24, fontweight="bold")
        ax.axis("off")
        plt.tight_layout()
        fig.savefig(outpath, transparent=False, facecolor="white")
        plt.close(fig)
        return

    wedges, _ = ax.pie(
        vals,
        labels=None,
        startangle=90,
        colors=colors,
        wedgeprops=dict(width=0.38, edgecolor="white", linewidth=1.2),
        shadow=True
    )

    for w, v in zip(wedges, vals):
        if v <= 0:
            continue
        ang = (w.theta2 + w.theta1) / 2.0
        x = 0.68 * np.cos(np.deg2rad(ang))
        y = 0.68 * np.sin(np.deg2rad(ang))
        ax.text(x, y, str(v), ha="center", va="center", fontsize=11, fontweight="bold", color="white")

    ax.text(0, 0.05, str(total), ha="center", va="center", fontsize=22, fontweight="bold")
    ax.text(0, -0.16, "Total", ha="center", va="center", fontsize=9, color="#666")

    ax.legend(
        wedges,
        [f"{l} ({v})" for l, v in zip(labels, vals)],
        loc="lower center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=2,
        frameon=False,
        fontsize=9
    )

    ax.set_aspect("equal")
    plt.tight_layout()
    fig.savefig(outpath, transparent=False, facecolor="white")
    plt.close(fig)


def _save_stacked_bar_visitas_ano(meses_labels, concluidas, pendentes, title, outpath):
    import os
    import numpy as np
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    
    # Barras empilhadas (Concluídas + Pendentes) por mês
    fig, ax = plt.subplots(figsize=(7.2, 3.4), dpi=160)

    x = np.arange(len(meses_labels))
    ax.bar(x, concluidas, label="Concluídas", color="#198754")
    ax.bar(x, pendentes, bottom=concluidas, label="Pendentes", color="#ffc107")

    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(meses_labels, rotation=0, fontsize=9)
    ax.grid(axis="y", alpha=0.25)
    ax.legend(frameon=False, ncol=2, loc="upper right")

    plt.tight_layout()
    fig.savefig(outpath, transparent=False, facecolor="white")
    plt.close(fig)

# =========================

from flask import send_file
from io import BytesIO
import calendar

# Matplotlib sem interface (obrigatório em servidor)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# =========================
# PDF - HELPERS
# =========================
from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import cm

labels_12 = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]


def _pt_month_name(m: int) -> str:
    nomes = [
        "Janeiro","Fevereiro","Março","Abril","Maio","Junho",
        "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"
    ]
    return nomes[m-1]


def _fmt_date_iso_to_br(s: str) -> str:
    # s = "YYYY-MM-DD" ou "YYYY-MM-DD HH:MM"
    if not s:
        return "—"
    try:
        yyyy, mm, dd = s[:10].split("-")
        return f"{dd}/{mm}/{yyyy}"
    except Exception:
        return s


def _table_style_light(repeat_header=True):
    return TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),

        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("TEXTCOLOR", (0, 1), (-1, -1), colors.HexColor("#111827")),

        ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#D0D7DE")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])


def _alternate_rows(table, n_rows):
    # zebra striping (linhas alternadas)
    cmds = []
    for r in range(1, n_rows):
        if r % 2 == 0:
            cmds.append(("BACKGROUND", (0, r), (-1, r), colors.HexColor("#FAFAFA")))
    table.setStyle(TableStyle(cmds))


def _make_donut(concluidas, pendentes, title):
    # gráfico limpo, com texto e fundo transparente (para encaixar bem no PDF)
    fig = plt.figure(figsize=(4.2, 3.2), dpi=160)
    ax = fig.add_subplot(111)

    vals = [max(0, concluidas), max(0, pendentes)]
    labels = ["Concluídas", "Pendentes"]

    # cores fixas (corporativo)
    colors_list = ["#16A34A", "#F59E0B"]

    wedges, _ = ax.pie(
        vals, startangle=90,
        colors=colors_list,
        wedgeprops=dict(width=0.42, edgecolor="white")
    )
    ax.set_title(title, fontsize=11, fontweight="bold", pad=10)

    total = sum(vals)
    ax.text(0, 0.05, str(total), ha="center", va="center", fontsize=16, fontweight="bold")
    ax.text(0, -0.15, "Total", ha="center", va="center", fontsize=9, color="#6B7280")

    # legenda discreta
    ax.legend(wedges, labels, loc="lower center", bbox_to_anchor=(0.5, -0.18), ncol=2, frameon=False)

    buf = BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", transparent=True)
    plt.close(fig)
    buf.seek(0)
    return buf

def _save_stacked_bar_visitas_ano(meses_labels, concluidas, pendentes, title, outpath):
    import os
    import numpy as np
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    # Força 12 itens
    labels = list(meses_labels)[:12]
    while len(labels) < 12:
        labels.append("")

    concl = np.array(concluidas, dtype=int).reshape(-1)[:12]
    pend = np.array(pendentes, dtype=int).reshape(-1)[:12]

    if concl.size < 12:
        concl = np.pad(concl, (0, 12 - concl.size), constant_values=0)
    if pend.size < 12:
        pend = np.pad(pend, (0, 12 - pend.size), constant_values=0)

    concl = np.maximum(concl, 0)
    pend = np.maximum(pend, 0)

    x = np.arange(12)

    fig = plt.figure(figsize=(8.4, 3.8), dpi=160)
    ax = plt.gca()

    ax.set_title(title, fontsize=11, fontweight="bold", pad=10)

    # Se tudo zero, ainda desenha um gráfico “vazio” sem quebrar
    ax.bar(x, concl, label="Concluídas", linewidth=0)
    ax.bar(x, pend, bottom=concl, label="Pendentes", linewidth=0)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.tick_params(axis="y", labelsize=9)

    ax.grid(axis="y", alpha=0.18)
    ax.legend(frameon=False, fontsize=9, loc="upper left")

    plt.tight_layout()
    fig.savefig(outpath, transparent=False, facecolor="white")
    plt.close(fig)


def _make_bar_corretivas(status_counts: dict, title: str):
    fig = plt.figure(figsize=(6.0, 3.2), dpi=160)
    ax = fig.add_subplot(111)

    ordem = ["Pendente", "Concluída", "BO", "OBS"]
    xs = ordem
    ys = [int(status_counts.get(k, 0)) for k in ordem]

    ax.bar(xs, ys)
    ax.set_title(title, fontsize=11, fontweight="bold", pad=10)
    ax.grid(axis="y", alpha=0.2)
    ax.set_ylabel("Qtd")

    buf = BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", transparent=True)
    plt.close(fig)
    buf.seek(0)
    return buf


def _make_bar_visitas(pendentes, concluidas, title: str):
    fig = plt.figure(figsize=(6.0, 3.2), dpi=160)
    ax = fig.add_subplot(111)

    xs = ["Pendentes", "Concluídas"]
    ys = [int(pendentes), int(concluidas)]
    ax.bar(xs, ys)
    ax.set_title(title, fontsize=11, fontweight="bold", pad=10)
    ax.grid(axis="y", alpha=0.2)
    ax.set_ylabel("Qtd")

    buf = BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", transparent=True)
    plt.close(fig)
    buf.seek(0)
    return buf


def _header_footer(canvas, doc, titulo):
    canvas.saveState()
    canvas.setFont("Helvetica", 9)
    canvas.setFillColor(colors.HexColor("#6B7280"))
    canvas.drawString(18, A4[1] - 18, titulo)
    canvas.drawRightString(A4[0] - 18, 14, f"Página {doc.page}")
    canvas.restoreState()

def _save_donut_chart(labels, values, colors, title, outpath):
    import os
    import numpy as np
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    # Sanitiza valores
    vals = []
    for v in values:
        try:
            v = int(v)
        except Exception:
            v = 0
        vals.append(max(0, v))

    total = sum(vals)

    fig = plt.figure(figsize=(6.2, 4.2), dpi=160)
    ax = plt.gca()

    ax.set_title(title, fontsize=11, fontweight="bold", pad=10)

    if total <= 0:
        # fallback "sem dados" (evita crash)
        ax.text(0.5, 0.52, "Sem dados", ha="center", va="center", fontsize=14, fontweight="bold")
        ax.text(0.5, 0.40, "0", ha="center", va="center", fontsize=24, fontweight="bold")
        ax.axis("off")
        plt.tight_layout()
        fig.savefig(outpath, transparent=False, facecolor="white")
        plt.close(fig)
        return

    # Pizza
    wedges, _ = ax.pie(
        vals,
        labels=None,
        startangle=90,
        colors=colors,
        wedgeprops=dict(width=0.38, edgecolor="white", linewidth=1.2),
        shadow=True
    )

    # Números dentro
    for w, v in zip(wedges, vals):
        if v <= 0:
            continue
        ang = (w.theta2 + w.theta1) / 2.0
        x = 0.68 * np.cos(np.deg2rad(ang))
        y = 0.68 * np.sin(np.deg2rad(ang))
        ax.text(x, y, str(v), ha="center", va="center", fontsize=11, fontweight="bold", color="white")

    # Total no centro
    ax.text(0, 0.05, str(total), ha="center", va="center", fontsize=22, fontweight="bold")
    ax.text(0, -0.16, "Total", ha="center", va="center", fontsize=9, color="#666")

    # Legenda
    ax.legend(
        wedges,
        [f"{l} ({v})" for l, v in zip(labels, vals)],
        loc="lower center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=2,
        frameon=False,
        fontsize=9
    )

    ax.set_aspect("equal")
    plt.tight_layout()
    fig.savefig(outpath, transparent=False, facecolor="white")
    plt.close(fig)

def gerar_relatorio_pdf(mes: int, ano: int):
    import os
    import numpy as np
    from io import BytesIO

    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.lib import colors

    # =========================
    # Referências
    # =========================
    ym = f"{ano:04d}-{mes:02d}"  # "2026-02"
    ano_ref = f"{ano:04d}"

    # pasta temporária p/ imagens
    tmp_dir = os.path.join(os.getcwd(), "tmp_relatorios")
    os.makedirs(tmp_dir, exist_ok=True)

    # =========================
    # Buscar dados do banco
    # =========================
    conn = get_db()
    c = conn.cursor()

    # contratos ativos
    contratos_ativos = c.execute("""
        SELECT id, nome_contrato, preventivas_mes
        FROM contratos
        WHERE ativo=1
        ORDER BY nome_contrato
    """).fetchall()

    total_previstas = sum(int(row["preventivas_mes"] or 0) for row in contratos_ativos)

    # preventivas do mês
    preventivas_mes = c.execute("""
        SELECT p.id, p.contrato_id, p.data_agendamento, p.status, ct.nome_contrato
        FROM preventivas p
        LEFT JOIN contratos ct ON ct.id = p.contrato_id
        WHERE strftime('%Y-%m', p.data_agendamento) = ?
        ORDER BY p.data_agendamento DESC
    """, (ym,)).fetchall()

    concluidas_prev = c.execute("""
        SELECT COUNT(*)
        FROM preventivas
        WHERE status='Concluída'
          AND strftime('%Y-%m', data_agendamento)=?
    """, (ym,)).fetchone()[0]

    pendentes_prev = max(0, int(total_previstas) - int(concluidas_prev))

    # corretivas do mês
    corretivas_mes = c.execute("""
        SELECT co.id, co.data, co.status, co.contato, co.ocorrencia, ct.nome_contrato
        FROM corretivas co
        LEFT JOIN contratos ct ON ct.id = co.contrato_id
        WHERE strftime('%Y-%m', co.data) = ?
        ORDER BY co.data DESC
    """, (ym,)).fetchall()

    cor_mes_conc = c.execute("""
        SELECT COUNT(*) FROM corretivas
        WHERE status='Concluída'
          AND strftime('%Y-%m', data)=?
    """, (ym,)).fetchone()[0]

    # pendente = tudo que não é concluída (inclui NULL/vazio/pendente/BO/OBS etc)
    cor_mes_pend = c.execute("""
        SELECT COUNT(*) FROM corretivas
        WHERE (status IS NULL OR TRIM(status) != 'Concluída')
          AND strftime('%Y-%m', data)=?
    """, (ym,)).fetchone()[0]

    # contagem status (para tabela/insights se você quiser)
    corretivas_counts = {"Pendente": 0, "Concluída": 0, "BO": 0, "OBS": 0}
    for row in corretivas_mes:
        st = (row["status"] or "Pendente").strip()
        if st not in corretivas_counts:
            st = "Pendente"
        corretivas_counts[st] += 1

    # visitas do mês
    visitas_mes = c.execute("""
        SELECT id, local, contato, data, hora, status, observacao
        FROM visitas_tecnicas
        WHERE strftime('%Y-%m', data) = ?
        ORDER BY data DESC, hora DESC
    """, (ym,)).fetchall()

    visitas_conc_mes = sum(1 for v in visitas_mes if (v["status"] or "").strip() == "Concluída")
    visitas_pend_mes = sum(1 for v in visitas_mes if (v["status"] or "").strip() != "Concluída")

    # visitas do ANO (12 meses) — SEMPRE 12
    labels_12 = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
    vis_conc_12 = [0]*12
    vis_pend_12 = [0]*12

    for i in range(12):
        ym_loop = f"{ano_ref}-{i+1:02d}"

        conc = c.execute("""
            SELECT COUNT(*) FROM visitas_tecnicas
            WHERE status='Concluída'
              AND strftime('%Y-%m', data)=?
        """, (ym_loop,)).fetchone()[0]

        pend = c.execute("""
            SELECT COUNT(*) FROM visitas_tecnicas
            WHERE (status IS NULL OR TRIM(status) != 'Concluída')
              AND strftime('%Y-%m', data)=?
        """, (ym_loop,)).fetchone()[0]

        vis_conc_12[i] = int(conc)
        vis_pend_12[i] = int(pend)

    conn.close()

    # =========================
    # Gerar imagens dos gráficos (salvar em PNG)
    # =========================
    # Preventivas donut (mês)
    img_prev = os.path.join(tmp_dir, "prev_donut.png")
    _save_donut_chart(
        labels=["Concluídas", "Pendentes"],
        values=[int(concluidas_prev), int(pendentes_prev)],
        colors=["#198754", "#ffc107"],
        title="Preventivas (mês) — Concluídas x Pendentes",
        outpath=img_prev
    )

    # Corretivas donut (mês) — igual preventiva
    img_cor = os.path.join(tmp_dir, "cor_donut.png")
    _save_donut_chart(
        labels=["Concluídas", "Pendentes"],
        values=[int(cor_mes_conc), int(cor_mes_pend)],
        colors=["#198754", "#ffc107"],
        title="Corretivas (mês) — Concluídas x Pendentes",
        outpath=img_cor
    )

    # Visitas ano (stacked)
    img_vis_ano = os.path.join(tmp_dir, "visitas_ano.png")
    _save_stacked_bar_visitas_ano(
        meses_labels=labels_12,
        concluidas=np.array(vis_conc_12, dtype=int),
        pendentes=np.array(vis_pend_12, dtype=int),
        title=f"Visitas técnicas ({ano_ref}) — Pendentes x Concluídas por mês",
        outpath=img_vis_ano
    )

    # =========================
    # Monta PDF (ReportLab)
    # =========================
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=18, rightMargin=18,
        topMargin=28, bottomMargin=22
    )

    styles = getSampleStyleSheet()

    h1 = ParagraphStyle(
        "h1", parent=styles["Heading1"],
        fontName="Helvetica-Bold", fontSize=16,
        textColor=colors.HexColor("#111827"),
        spaceAfter=10
    )
    h2 = ParagraphStyle(
        "h2", parent=styles["Heading2"],
        fontName="Helvetica-Bold", fontSize=12,
        textColor=colors.HexColor("#111827"),
        spaceBefore=12, spaceAfter=6
    )
    body = ParagraphStyle(
        "body", parent=styles["BodyText"],
        fontName="Helvetica", fontSize=9,
        leading=11, textColor=colors.HexColor("#111827")
    )
    muted = ParagraphStyle(
        "muted", parent=styles["BodyText"],
        fontName="Helvetica", fontSize=8,
        leading=10, textColor=colors.HexColor("#6B7280")
    )

    titulo = f"HAFAR Manutenções — Relatório Mensal ({_pt_month_name(mes)} / {ano})"
    story = []

    story.append(Paragraph(titulo, h1))
    story.append(Paragraph(f"Referência: <b>{_pt_month_name(mes)} de {ano}</b> (mês/ano selecionado)", muted))
    story.append(Spacer(1, 10))

    # KPIs
    story.append(Paragraph("Resumo", h2))
    kpi_data = [
        ["Preventivas previstas", str(total_previstas)],
        ["Preventivas concluídas", str(concluidas_prev)],
        ["Preventivas pendentes", str(pendentes_prev)],
        ["Corretivas registradas", str(len(corretivas_mes))],
        ["Visitas registradas", str(len(visitas_mes))],
        ["Visitas pendentes", str(visitas_pend_mes)],
    ]
    kpi_table = Table(kpi_data, colWidths=[7.5*cm, 3.0*cm])
    kpi_table.setStyle(TableStyle([
        ("GRID", (0,0), (-1,-1), 0.6, colors.HexColor("#D0D7DE")),
        ("BACKGROUND", (0,0), (-1,-1), colors.white),
        ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("LEFTPADDING", (0,0), (-1,-1), 6),
        ("RIGHTPADDING", (0,0), (-1,-1), 6),
        ("TOPPADDING", (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
    ]))
    story.append(kpi_table)
    story.append(Spacer(1, 12))

    # Gráficos
    story.append(Paragraph("Gráficos", h2))
    charts = Table(
        [[
            RLImage(img_prev, width=8.5*cm, height=6.0*cm),
            RLImage(img_cor,  width=10.5*cm, height=6.0*cm),
        ]],
        colWidths=[8.8*cm, 10.2*cm]
    )
    charts.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "MIDDLE")]))
    story.append(charts)
    story.append(Spacer(1, 8))

    story.append(RLImage(img_vis_ano, width=18.6*cm, height=7.6*cm))
    story.append(Spacer(1, 10))

    # Tabela Preventivas
    story.append(Paragraph("Preventivas (mês)", h2))
    prev_data = [["Contrato", "Data", "Status"]]
    for p in preventivas_mes:
        prev_data.append([
            p["nome_contrato"] or "—",
            _fmt_date_iso_to_br(p["data_agendamento"]),
            p["status"] or "—"
        ])
    if len(prev_data) == 1:
        prev_data.append(["—", "—", "—"])

    t_prev = Table(prev_data, colWidths=[10.5*cm, 3.0*cm, 3.0*cm], repeatRows=1)
    t_prev.setStyle(_table_style_light())
    _alternate_rows(t_prev, len(prev_data))
    story.append(t_prev)
    story.append(Spacer(1, 10))

    # Tabela Corretivas
    story.append(Paragraph("Corretivas (mês)", h2))
    corr_data = [["Contrato", "Data", "Status", "Contato", "Ocorrência"]]
    for co in corretivas_mes:
        corr_data.append([
            co["nome_contrato"] or "—",
            _fmt_date_iso_to_br(co["data"]),
            (co["status"] or "Pendente"),
            (co["contato"] or "—"),
            Paragraph(str(co["ocorrencia"] or ""), body),
        ])
    if len(corr_data) == 1:
        corr_data.append(["—", "—", "—", "—", Paragraph("—", body)])

    t_corr = Table(corr_data, colWidths=[5.0*cm, 2.4*cm, 2.4*cm, 3.0*cm, 6.2*cm], repeatRows=1)
    t_corr.setStyle(_table_style_light())
    _alternate_rows(t_corr, len(corr_data))
    story.append(t_corr)
    story.append(Spacer(1, 10))

    # Tabela Visitas
    story.append(Paragraph("Visitas Técnicas (mês)", h2))
    vis_data = [["Local", "Contato", "Data", "Hora", "Status", "Observação"]]
    for v in visitas_mes:
        vis_data.append([
            Paragraph(str(v["local"] or "—"), body),
            Paragraph(str(v["contato"] or "—"), body),
            _fmt_date_iso_to_br(v["data"]),
            (v["hora"] or "—"),
            (v["status"] or "Pendente"),
            Paragraph(str(v["observacao"] or ""), body),
        ])
    if len(vis_data) == 1:
        vis_data.append([Paragraph("—", body), Paragraph("—", body), "—", "—", "—", Paragraph("—", body)])

    t_vis = Table(
        vis_data,
        colWidths=[4.2*cm, 3.0*cm, 2.2*cm, 1.6*cm, 2.2*cm, 5.8*cm],
        repeatRows=1
    )
    t_vis.setStyle(_table_style_light())
    _alternate_rows(t_vis, len(vis_data))
    story.append(t_vis)

    doc.build(
        story,
        onFirstPage=lambda canv, d: _header_footer(canv, d, titulo),
        onLaterPages=lambda canv, d: _header_footer(canv, d, titulo),
    )

    buffer.seek(0)
    return buffer


# =========================
# ROTAS PDF (tela + download)
# =========================

from flask import render_template, request, send_file

@app.route("/relatorio_pdf")
def relatorio_pdf():
    hoje = datetime.now()
    mes = int(request.args.get("mes", hoje.month))
    ano = int(request.args.get("ano", hoje.year))

    download = request.args.get("download", "0") == "1"
    if download:
        pdf = gerar_relatorio_pdf(mes, ano)
        filename = f"Relatorio_HAFAR_{ano:04d}-{mes:02d}.pdf"
        return send_file(
            pdf,
            as_attachment=True,
            download_name=filename,
            mimetype="application/pdf"
        )

    # lista de meses pro select
    meses = [(m, _pt_month_name(m)) for m in range(1, 13)]

    return render_template(
        "relatorio_pdf.html",
        mes=mes,
        ano=ano,
        meses=meses
    )

if __name__ == "__main__":
    app.run(debug=False)
