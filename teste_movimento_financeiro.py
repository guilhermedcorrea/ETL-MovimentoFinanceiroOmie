import json
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time as dtime
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from typing import List

load_dotenv()




"""
Esse codigo verifica o ultimo ponto em que fez a atualização com os dados vindo da api

https://app.omie.com.br/api/v1/financas/mf/

Movimentos Financeiros do OMIE

A idéia é a cada atualização o pipeline nao precisar varrer todo o omie para puxar as informaçoes
ou seja ele pega apenas os dados que tiveram atualização



Obs: O  documento financeiro pode possuir vários eventos relacionados a ele,
mas todos podem compartilhar a mesma DocumentoKey.

Essa chave só é criada quando existe nCodTitulo.


O EVENTKEY: É uma chave que identifica um evento financeiro específico.

Enquanto DocumentoKey identifica o título, EventKey identifica a ocorrência financeira associada a ele.


"""







URL_OMIE_MF = "https://app.omie.com.br/api/v1/financas/mf/"

JSON_KWARGS = {
    "ensure_ascii": False,
    "separators": (",", ":"),
}



"""Classe
Configura e seleciona ambiente Omie

"""



@dataclass(frozen=True)
class AmbienteOmie:
    id_empresa_proprietaria: int
    app_key: str
    app_secret: str





"""Conexão api Omie"""
AMBIENTES: List[AmbienteOmie] = [
    AmbienteOmie(
        id_empresa_proprietaria=1,
        app_key=os.environ["APP_KEY_SH53"],
        app_secret=os.environ["APP_SECRET_SH53"],
    ),
    AmbienteOmie(
        id_empresa_proprietaria=2,
        app_key=os.environ["APP_KEY_SINAMOVEL"],
        app_secret=os.environ["APP_SECRET_SINAMOVEL"],
    ),
]

TIPOS_LANCAMENTO = ["CPCR", "BX", "CC"]



"""Tentativas e Paginação

define 6 parâmetros de configuração da integração entre o Omie e o SQL Server.
Eles controlam o período de consulta, a quantidade de registros por requisição
as tentativas em caso de erro e a data inicial da extração.

OVERLAP_DIAS = 3

reprocessa os últimos três dias anteriores à data registrada no watermark.

WATERMARK: registra até que ponto a extração foi realizada.

"""


OVERLAP_DIAS = 3
REGISTROS_POR_PAGINA = 500
MAX_TENTATIVAS_DB = 5
MAX_TENTATIVAS_OMIE = 5

PRIMEIRA_CARGA_DESDE = date(2020, 1, 1)
JANELA_DIAS = 30





params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={os.environ['SERVER']};"
    f"DATABASE={os.environ['DATABASE']};"
    f"UID={os.environ['UID']};"
    f"PWD={os.environ['PWD']};"
    "Connection Timeout=30;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=1800,
    future=True,
)





def criar_sessao_http() -> requests.Session:
    sess = requests.Session()

    retry = Retry(
        total=6,
        backoff_factor=1.1,
        status_forcelist=[425, 429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    sess.mount("https://", adapter)
    sess.headers.update(
        {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "integracao-omie-mf/1.0",
        }
    )
    return sess


HTTP = criar_sessao_http()



def parse_data_ddmmaaaa(valor: Optional[str]) -> Optional[date]:
    if not valor:
        return None
    return datetime.strptime(valor, "%d/%m/%Y").date()

def parse_hora_hhmmss(valor: Optional[str]) -> Optional[dtime]:
    if not valor:
        return None
    fmt = "%H:%M:%S" if valor.count(":") == 2 else "%H:%M"
    return datetime.strptime(valor, fmt).time()

def combinar_data_hora(d: Optional[date], h: Optional[dtime]) -> Optional[datetime]:
    if d is None:
        return None
    if h is None:
        return datetime(d.year, d.month, d.day, 0, 0, 0)
    return datetime(d.year, d.month, d.day, h.hour, h.minute, h.second)




ESCALA_DECIMAL_4 = Decimal("0.0001")

def _quantizar_decimal_4(d: Decimal) -> Optional[Decimal]:
    try:
        return d.quantize(ESCALA_DECIMAL_4, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None

def to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, (float, Decimal)):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        neg = s.startswith("-")
        s = "".join(ch for ch in s if ch.isdigit())
        if not s:
            return None
        n = int(s)
        return -n if neg else n
    return None

def to_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None

        s = s.replace("R$", "").replace(" ", "")

        if "." in s and "," in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")

        try:
            return Decimal(s)
        except InvalidOperation:
            return None
    return None

def to_decimal_18_4(v: Any) -> Optional[Decimal]:
    d = to_decimal(v)
    if d is None:
        return None

    d = _quantizar_decimal_4(d)
    if d is None:
        return None

    limite = Decimal("99999999999999.9999")
    if d.copy_abs() > limite:
        return None

    return d




"""Verifiva se existe a WATERMARK se nao existir cria"""


DDL_WATERMARK = """
IF OBJECT_ID('dbo.MovFinWatermark', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.MovFinWatermark
    (
        IDEmpresaProprietaria INT NOT NULL,
        TipoLancamento  VARCHAR(10) NOT NULL,
        UltimaDataHora  DATETIME2(0) NOT NULL,
        DataHoraAtualizacao  DATETIME2(0) NOT NULL CONSTRAINT DF_MovFinWatermark_DataHora DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_MovFinWatermark PRIMARY KEY (IDEmpresaProprietaria, TipoLancamento)
    );
END
"""

def garantir_tabela_watermark(conn) -> None:
    conn.execute(text(DDL_WATERMARK))



"""
tabela: MovFinWatermark

Verifica a WATERMARK: Checa a ultima configuração dela.

Esse trecho tem duas funções: uma consulta a última data de sincronização no SQL Server
e a outra atualiza essa data.

"""


def obter_watermark(conn, id_empresa: int, tipo: str) -> datetime:
    sql = text("""
        SELECT UltimaDataHora
        FROM dbo.MovFinWatermark
        WHERE IDEmpresaProprietaria = :id_empresa AND TipoLancamento = :tipo
    """)
    row = conn.execute(sql, {"id_empresa": id_empresa, "tipo": tipo}).fetchone()
    if row and row[0]:
        return row[0]
    return datetime(PRIMEIRA_CARGA_DESDE.year, PRIMEIRA_CARGA_DESDE.month, PRIMEIRA_CARGA_DESDE.day, 0, 0, 0)

def atualizar_watermark(conn, id_empresa: int, tipo: str, nova_datahora: datetime) -> None:
    sql = text("""
        MERGE dbo.MovFinWatermark AS tgt
        USING (SELECT :id_empresa AS IDEmpresaProprietaria,
                      :tipo AS TipoLancamento,
                      :dt  AS UltimaDataHora) AS src
        ON (tgt.IDEmpresaProprietaria = src.IDEmpresaProprietaria AND tgt.TipoLancamento = src.TipoLancamento)
        WHEN MATCHED THEN
            UPDATE SET UltimaDataHora = src.UltimaDataHora, DataHoraAtualizacao = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (IDEmpresaProprietaria, TipoLancamento, UltimaDataHora)
            VALUES (src.IDEmpresaProprietaria, src.TipoLancamento, src.UltimaDataHora);
    """)
    conn.execute(sql, {"id_empresa": id_empresa, "tipo": tipo, "dt": nova_datahora})



def _debug_http_error(resp: requests.Response, payload: Dict[str, Any]) -> str:
    try:
        body = resp.text or ""
    except Exception:
        body = "<sem body>"

    payload_resumo = {
        "call": payload.get("call"),
        "param": payload.get("param"),
        "app_key": "***",
        "app_secret": "***",
    }

    return (
        f"HTTP {resp.status_code} ao chamar Omie.\n"
        f"Body: {body[:2000]}\n"
        f"Payload(resumo): {json.dumps(payload_resumo, **JSON_KWARGS)[:2000]}"
    )

def omie_listar_movimentos(
    ambiente: AmbienteOmie,
    tipo_lancamento: str,
    pagina: int,
    reg_por_pagina: int,
    dt_alt_de: date,
    dt_alt_ate: date,
) -> Dict[str, Any]:

    payload = {
        "call": "ListarMovimentos",
        "app_key": ambiente.app_key,
        "app_secret": ambiente.app_secret,
        "param": [
            {
                "nPagina": pagina,
                "nRegPorPagina": reg_por_pagina,
                "cTpLancamento": tipo_lancamento,
                "lDadosCad": "S",
                "cExibirDepartamentos": "S",
                "dDtAltDe": dt_alt_de.strftime("%d/%m/%Y"),
                "dDtAltAte": dt_alt_ate.strftime("%d/%m/%Y"),
            }
        ],
    }

    for tentativa in range(1, MAX_TENTATIVAS_OMIE + 1):
        resp = HTTP.post(URL_OMIE_MF, json=payload, timeout=90)

        if resp.status_code >= 400:
            if tentativa == MAX_TENTATIVAS_OMIE:
                raise RuntimeError(_debug_http_error(resp, payload))
            time.sleep(1.5 * tentativa)
            continue

        try:
            data = resp.json()
        except Exception:
            if tentativa == MAX_TENTATIVAS_OMIE:
                raise RuntimeError(_debug_http_error(resp, payload))
            time.sleep(1.5 * tentativa)
            continue

        if isinstance(data, dict) and ("omie_fail" in data or "faultstring" in data or "faultcode" in data):
            if tentativa == MAX_TENTATIVAS_OMIE:
                raise RuntimeError(f"Erro Omie: {json.dumps(data, ensure_ascii=False, indent=2)[:4000]}")
            time.sleep(1.5 * tentativa)
            continue

        return data

    raise RuntimeError("Falha inesperada ao chamar Omie.")



def montar_tipo_evento_e_chaves(
    id_empresa: int,
    tipo_api: str,
    nCodBaixa: Optional[int],
    nCodMovCC: Optional[int],
    nCodTitulo: Optional[int],
    cGrupo: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    
    grupo = (cGrupo or "").strip()

    tipo_lancamento = (tipo_api or "").strip() or None

    if nCodBaixa is not None:
        event_key = f"{id_empresa}|BX|{nCodBaixa}"
    elif nCodMovCC is not None:
        event_key = f"{id_empresa}|CC|{nCodMovCC}"
    elif nCodTitulo is not None:
    
        event_key = f"{id_empresa}|{tipo_api}|{grupo}|{nCodTitulo}"
    else:
        event_key = None

    documento_key = None
    if nCodTitulo is not None:
        documento_key = f"{id_empresa}|{grupo}|TIT|{nCodTitulo}"

    return tipo_lancamento, event_key, documento_key





"""
Prepara dados e executa ETL na tabela MovimentacaoFinanceiro

Faz tratamento do JSON, organiza e normaliza campos

IDENTIFICA DEC_COLS e INT_COLS


DEC_COLS: campos numéricos decimais, principalmente valores financeiros.
INT_COLS: campos numéricos inteiros, principalmente identificadores e códigos.

E faz o UPSERT

"""



COLUNAS_TABELA = [
    "IDEmpresaProprietaria",

    "TipoLancamento",
    "EventKey",
    "DocumentoKey",

    "nPagina", "nTotPaginas", "nRegistros", "nTotRegistros",

    "nCodTitulo", "cCodIntTitulo", "cNumTitulo",
    "dDtEmissao", "dDtVenc", "dDtPrevisao", "dDtPagamento",
    "nCodCliente", "cCPFCNPJCliente",
    "nCodCtr", "cNumCtr",
    "nCodOS", "cNumOS",
    "nCodCC",
    "cStatus", "cNatureza", "cTipo", "cOperacao",
    "cNumDocFiscal", "cCodCateg",
    "cNumParcela",
    "nValorTitulo",

    "nValorPIS", "cRetPIS",
    "nValorCOFINS", "cRetCOFINS",
    "nValorCSLL", "cRetCSLL",
    "nValorIR", "cRetIR",
    "nValorISS", "cRetISS",
    "nValorINSS", "cRetINSS",

    "nCodProjeto",
    "observacao",
    "cCodVendedor", "nCodComprador",
    "cCodigoBarras", "cNSU",
    "nCodNF", "dDtRegistro",
    "cNumBoleto", "cChaveNFe",
    "cOrigem",
    "nCodTitRepet", "cGrupo",
    "nCodMovCC", "nValorMovCC", "nCodMovCCRepet",
    "nCodBaixa",
    "dDtCredito",
    "dDtConcilia", "cHrConcilia", "cUsConcilia",
    "dDtInc", "cHrInc", "cUsInc",
    "dDtAlt", "cHrAlt", "cUsAlt",

    "cLiquidado",
    "nValPago", "nValAberto", "nDesconto", "nJuros", "nMulta", "nValLiquido",

    "CategoriasJson", "DepartamentosJson",
    "MovimentoJson",
]

DEC_COLS = {
    "nValorTitulo",
    "nValorPIS",
    "nValorCOFINS",
    "nValorCSLL",
    "nValorIR",
    "nValorISS",
    "nValorINSS",
    "nValorMovCC",
    "nValPago",
    "nValAberto",
    "nDesconto",
    "nJuros",
    "nMulta",
    "nValLiquido",
}

INT_COLS = {
    "IDEmpresaProprietaria",
    "nPagina", "nTotPaginas", "nRegistros", "nTotRegistros",
    "nCodTitulo", "nCodCliente", "nCodCtr", "nCodOS", "nCodCC",
    "nCodProjeto", "cCodVendedor", "nCodComprador",
    "nCodNF", "nCodTitRepet",
    "nCodMovCC", "nCodMovCCRepet", "nCodBaixa",
}

def _normalizar_tipos(linha: Dict[str, Any]) -> None:
    for c in DEC_COLS:
        linha[c] = to_decimal_18_4(linha.get(c))
    for c in INT_COLS:
        linha[c] = to_int(linha.get(c))

def achatar_movimento(
    id_empresa: int,
    meta: Dict[str, Any],
    movimento: Dict[str, Any],
    tipo_lancamento_api: str,
) -> Dict[str, Any]:
    detalhes = movimento.get("detalhes", {}) or {}
    resumo = movimento.get("resumo", {}) or {}
    categorias = movimento.get("categorias", None)
    departamentos = movimento.get("departamentos", None)

    linha: Dict[str, Any] = {}

    linha["IDEmpresaProprietaria"] = id_empresa
    linha["nPagina"] = meta.get("nPagina")
    linha["nTotPaginas"] = meta.get("nTotPaginas")
    linha["nRegistros"] = meta.get("nRegistros")
    linha["nTotRegistros"] = meta.get("nTotRegistros")

    linha["nCodTitulo"] = detalhes.get("nCodTitulo")
    linha["cCodIntTitulo"] = detalhes.get("cCodIntTitulo")
    linha["cNumTitulo"] = detalhes.get("cNumTitulo")

    linha["dDtEmissao"] = parse_data_ddmmaaaa(detalhes.get("dDtEmissao"))
    linha["dDtVenc"] = parse_data_ddmmaaaa(detalhes.get("dDtVenc"))
    linha["dDtPrevisao"] = parse_data_ddmmaaaa(detalhes.get("dDtPrevisao"))
    linha["dDtPagamento"] = parse_data_ddmmaaaa(detalhes.get("dDtPagamento"))

    linha["nCodCliente"] = detalhes.get("nCodCliente")
    linha["cCPFCNPJCliente"] = detalhes.get("cCPFCNPJCliente")

    linha["nCodCtr"] = detalhes.get("nCodCtr")
    linha["cNumCtr"] = detalhes.get("cNumCtr")

    linha["nCodOS"] = detalhes.get("nCodOS")
    linha["cNumOS"] = detalhes.get("cNumOS")

    linha["nCodCC"] = detalhes.get("nCodCC")

    linha["cStatus"] = detalhes.get("cStatus")
    linha["cNatureza"] = detalhes.get("cNatureza")
    linha["cTipo"] = detalhes.get("cTipo")
    linha["cOperacao"] = detalhes.get("cOperacao")

    linha["cNumDocFiscal"] = detalhes.get("cNumDocFiscal")
    linha["cCodCateg"] = detalhes.get("cCodCateg")

    linha["cNumParcela"] = detalhes.get("cNumParcela")
    linha["nValorTitulo"] = detalhes.get("nValorTitulo")

    linha["nValorPIS"] = detalhes.get("nValorPIS")
    linha["cRetPIS"] = detalhes.get("cRetPIS")

    linha["nValorCOFINS"] = detalhes.get("nValorCOFINS")
    linha["cRetCOFINS"] = detalhes.get("cRetCOFINS")

    linha["nValorCSLL"] = detalhes.get("nValorCSLL")
    linha["cRetCSLL"] = detalhes.get("cRetCSLL")

    linha["nValorIR"] = detalhes.get("nValorIR")
    linha["cRetIR"] = detalhes.get("cRetIR")

    linha["nValorISS"] = detalhes.get("nValorISS")
    linha["cRetISS"] = detalhes.get("cRetISS")

    linha["nValorINSS"] = detalhes.get("nValorINSS")
    linha["cRetINSS"] = detalhes.get("cRetINSS")

    linha["nCodProjeto"] = detalhes.get("nCodProjeto")
    linha["observacao"] = detalhes.get("observacao")

    linha["cCodVendedor"] = detalhes.get("cCodVendedor")
    linha["nCodComprador"] = detalhes.get("nCodComprador")

    linha["cCodigoBarras"] = detalhes.get("cCodigoBarras")
    linha["cNSU"] = detalhes.get("cNSU")

    linha["nCodNF"] = detalhes.get("nCodNF")
    linha["dDtRegistro"] = parse_data_ddmmaaaa(detalhes.get("dDtRegistro"))

    linha["cNumBoleto"] = detalhes.get("cNumBoleto")
    linha["cChaveNFe"] = detalhes.get("cChaveNFe")

    linha["cOrigem"] = detalhes.get("cOrigem")

    linha["nCodTitRepet"] = detalhes.get("nCodTitRepet")
    linha["cGrupo"] = detalhes.get("cGrupo")

    linha["nCodMovCC"] = detalhes.get("nCodMovCC")
    linha["nValorMovCC"] = detalhes.get("nValorMovCC")
    linha["nCodMovCCRepet"] = detalhes.get("nCodMovCCRepet")

    linha["nCodBaixa"] = detalhes.get("nCodBaixa")

    linha["dDtCredito"] = parse_data_ddmmaaaa(detalhes.get("dDtCredito"))

    linha["dDtConcilia"] = parse_data_ddmmaaaa(detalhes.get("dDtConcilia"))
    linha["cHrConcilia"] = parse_hora_hhmmss(detalhes.get("cHrConcilia"))
    linha["cUsConcilia"] = detalhes.get("cUsConcilia")

    linha["dDtInc"] = parse_data_ddmmaaaa(detalhes.get("dDtInc"))
    linha["cHrInc"] = parse_hora_hhmmss(detalhes.get("cHrInc"))
    linha["cUsInc"] = detalhes.get("cUsInc")

    linha["dDtAlt"] = parse_data_ddmmaaaa(detalhes.get("dDtAlt"))
    linha["cHrAlt"] = parse_hora_hhmmss(detalhes.get("cHrAlt"))
    linha["cUsAlt"] = detalhes.get("cUsAlt")

    linha["cLiquidado"] = resumo.get("cLiquidado")
    linha["nValPago"] = resumo.get("nValPago")
    linha["nValAberto"] = resumo.get("nValAberto")
    linha["nDesconto"] = resumo.get("nDesconto")
    linha["nJuros"] = resumo.get("nJuros")
    linha["nMulta"] = resumo.get("nMulta")
    linha["nValLiquido"] = resumo.get("nValLiquido")

    linha["CategoriasJson"] = json.dumps(categorias, **JSON_KWARGS) if categorias is not None else None
    linha["DepartamentosJson"] = json.dumps(departamentos, **JSON_KWARGS) if departamentos is not None else None
    linha["MovimentoJson"] = json.dumps(movimento, **JSON_KWARGS)

    _normalizar_tipos(linha)

    """
    cria as chaves EVENTKEY e DOCUMENTKEY
    identificar registros financeiros
    relacionar informações e evitar que movimentações diferentes sejam confundidas ou duplicadas.

    1. DocumentoKey — Identifica o documento financeiro
    É uma chave criada para identificar um título financeiro dentro da integração.
    O documento pode representar uma conta a pagar ou uma conta a receber.

    
    Permite relacionar diferentes registros financeiros que pertencem ao mesmo título.


    Um único título pode estar associado a vários movimentos, como pagamentos, recebimentos e baixas. 
    A DocumentoKey permite identificar essa relação.


    2. EventKey — Identifica o evento financeiro

    É uma chave utilizada para identificar uma ocorrência financeira específica

    Sua principal finalidade é permitir que o ETL reconheça os eventos que já foram gravados no SQL Server.
    Assim evitando duplicidade

    
    1. nCodBaixa: identifica o evento pelo código da baixa financeira.
    2. nCodMovCC: identifica o evento pelo código da movimentação de conta corrente.
    3. nCodTitulo: utiliza o título, o tipo de lançamento e o grupo quando os identificadores anteriores não estão disponíveis.
    A EventKey é especialmente importante porque o MERGE utiliza essa chave para encontrar registros existentes.
    
    Vale observar que a chave só evita duplicidades quando sua composição identifica cada evento de maneira única e consistente.


    3. IdChaveOmie — Identificador financeiro auxiliar

    coluna calculada automaticamente pelo SQL Server.
    Sua fórmula é:
    COALESCE(nCodBaixa, nCodMovCC, nCodTitulo)


    
    """
  
    tipo_lanc, event_key, doc_key = montar_tipo_evento_e_chaves(
        id_empresa=id_empresa,
        tipo_api=tipo_lancamento_api,
        nCodBaixa=linha.get("nCodBaixa"),
        nCodMovCC=linha.get("nCodMovCC"),
        nCodTitulo=linha.get("nCodTitulo"),
        cGrupo=linha.get("cGrupo"),
    )
    linha["TipoLancamento"] = tipo_lanc
    linha["EventKey"] = event_key
    linha["DocumentoKey"] = doc_key

    return linha

def extrair_momento_alteracao(linha: Dict[str, Any]) -> Optional[datetime]:
    dt_alt = combinar_data_hora(linha.get("dDtAlt"), linha.get("cHrAlt"))
    if dt_alt:
        return dt_alt
    return combinar_data_hora(linha.get("dDtInc"), linha.get("cHrInc"))



def upsert_pagina(conn, linhas: List[Dict[str, Any]]) -> None:
    if not linhas:
        return

    linhas_validas = [l for l in linhas if l.get("EventKey")]
    if not linhas_validas:
        return

    conn.execute(text("""
        IF OBJECT_ID('tempdb..#MovFinStage') IS NOT NULL DROP TABLE #MovFinStage;

        CREATE TABLE #MovFinStage
        (
            IDEmpresaProprietaria INT NOT NULL,

            TipoLancamento VARCHAR(10) NULL,
            EventKey        VARCHAR(200) NULL,
            DocumentoKey    VARCHAR(200) NULL,

            nPagina INT NULL,
            nTotPaginas INT NULL,
            nRegistros INT NULL,
            nTotRegistros INT NULL,

            nCodTitulo BIGINT NULL,
            cCodIntTitulo VARCHAR(80) NULL,
            cNumTitulo VARCHAR(40) NULL,

            dDtEmissao DATE NULL,
            dDtVenc DATE NULL,
            dDtPrevisao DATE NULL,
            dDtPagamento DATE NULL,

            nCodCliente BIGINT NULL,
            cCPFCNPJCliente VARCHAR(30) NULL,

            nCodCtr BIGINT NULL,
            cNumCtr VARCHAR(30) NULL,

            nCodOS BIGINT NULL,
            cNumOS VARCHAR(20) NULL,

            nCodCC BIGINT NULL,

            cStatus VARCHAR(120) NULL,
            cNatureza CHAR(1) NULL,
            cTipo VARCHAR(20) NULL,
            cOperacao VARCHAR(10) NULL,

            cNumDocFiscal VARCHAR(40) NULL,
            cCodCateg VARCHAR(40) NULL,

            cNumParcela VARCHAR(15) NULL,

            nValorTitulo DECIMAL(18,4) NULL,

            nValorPIS DECIMAL(18,4) NULL,
            cRetPIS CHAR(1) NULL,

            nValorCOFINS DECIMAL(18,4) NULL,
            cRetCOFINS CHAR(1) NULL,

            nValorCSLL DECIMAL(18,4) NULL,
            cRetCSLL CHAR(1) NULL,

            nValorIR DECIMAL(18,4) NULL,
            cRetIR CHAR(1) NULL,

            nValorISS DECIMAL(18,4) NULL,
            cRetISS CHAR(1) NULL,

            nValorINSS DECIMAL(18,4) NULL,
            cRetINSS CHAR(1) NULL,

            nCodProjeto BIGINT NULL,
            observacao VARCHAR(MAX) NULL,

            cCodVendedor BIGINT NULL,
            nCodComprador BIGINT NULL,

            cCodigoBarras VARCHAR(100) NULL,
            cNSU VARCHAR(120) NULL,

            nCodNF BIGINT NULL,
            dDtRegistro DATE NULL,

            cNumBoleto VARCHAR(50) NULL,
            cChaveNFe VARCHAR(60) NULL,

            cOrigem VARCHAR(10) NULL,

            nCodTitRepet BIGINT NULL,
            cGrupo VARCHAR(40) NULL,

            nCodMovCC BIGINT NULL,
            nValorMovCC DECIMAL(18,4) NULL,
            nCodMovCCRepet BIGINT NULL,

            nCodBaixa BIGINT NULL,

            dDtCredito DATE NULL,

            dDtConcilia DATE NULL,
            cHrConcilia TIME(0) NULL,
            cUsConcilia VARCHAR(30) NULL,

            dDtInc DATE NULL,
            cHrInc TIME(0) NULL,
            cUsInc VARCHAR(30) NULL,

            dDtAlt DATE NULL,
            cHrAlt TIME(0) NULL,
            cUsAlt VARCHAR(30) NULL,

            cLiquidado CHAR(1) NULL,
            nValPago DECIMAL(18,4) NULL,
            nValAberto DECIMAL(18,4) NULL,
            nDesconto DECIMAL(18,4) NULL,
            nJuros DECIMAL(18,4) NULL,
            nMulta DECIMAL(18,4) NULL,
            nValLiquido DECIMAL(18,4) NULL,

            CategoriasJson NVARCHAR(MAX) NULL,
            DepartamentosJson NVARCHAR(MAX) NULL,
            MovimentoJson NVARCHAR(MAX) NULL
        );
    """))

    import pyodbc
    raw_conn = conn.connection
    cur = raw_conn.cursor()
    cur.fast_executemany = True

    cols = COLUNAS_TABELA
    placeholders = ",".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO #MovFinStage ({','.join(cols)}) VALUES ({placeholders})"
    valores: List[Tuple[Any, ...]] = [tuple(l.get(c) for c in cols) for l in linhas_validas]



    VARCHAR_BIND_MAX = 8000
    NVARCHAR_BIND_MAX = 4000

    TYPE_BY_COL: Dict[str, Tuple[Any, ...]] = {
        # ints
        "IDEmpresaProprietaria": (pyodbc.SQL_INTEGER,),
        "nPagina": (pyodbc.SQL_INTEGER,),
        "nTotPaginas": (pyodbc.SQL_INTEGER,),
        "nRegistros": (pyodbc.SQL_INTEGER,),
        "nTotRegistros": (pyodbc.SQL_INTEGER,),

        # bigints
        "nCodTitulo": (pyodbc.SQL_BIGINT,),
        "nCodCliente": (pyodbc.SQL_BIGINT,),
        "nCodCtr": (pyodbc.SQL_BIGINT,),
        "nCodOS": (pyodbc.SQL_BIGINT,),
        "nCodCC": (pyodbc.SQL_BIGINT,),
        "nCodProjeto": (pyodbc.SQL_BIGINT,),
        "cCodVendedor": (pyodbc.SQL_BIGINT,),
        "nCodComprador": (pyodbc.SQL_BIGINT,),
        "nCodNF": (pyodbc.SQL_BIGINT,),
        "nCodTitRepet": (pyodbc.SQL_BIGINT,),
        "nCodMovCC": (pyodbc.SQL_BIGINT,),
        "nCodMovCCRepet": (pyodbc.SQL_BIGINT,),
        "nCodBaixa": (pyodbc.SQL_BIGINT,),

        # dates (SIZE 10)
        "dDtEmissao": (pyodbc.SQL_TYPE_DATE, 10),
        "dDtVenc": (pyodbc.SQL_TYPE_DATE, 10),
        "dDtPrevisao": (pyodbc.SQL_TYPE_DATE, 10),
        "dDtPagamento": (pyodbc.SQL_TYPE_DATE, 10),
        "dDtRegistro": (pyodbc.SQL_TYPE_DATE, 10),
        "dDtCredito": (pyodbc.SQL_TYPE_DATE, 10),
        "dDtConcilia": (pyodbc.SQL_TYPE_DATE, 10),
        "dDtInc": (pyodbc.SQL_TYPE_DATE, 10),
        "dDtAlt": (pyodbc.SQL_TYPE_DATE, 10),

        # times TIME(0) (SIZE 8, SCALE 0)
        "cHrConcilia": (pyodbc.SQL_TYPE_TIME, 8, 0),
        "cHrInc": (pyodbc.SQL_TYPE_TIME, 8, 0),
        "cHrAlt": (pyodbc.SQL_TYPE_TIME, 8, 0),

        # decimals
        "nValorTitulo": (pyodbc.SQL_DECIMAL, 18, 4),
        "nValorPIS": (pyodbc.SQL_DECIMAL, 18, 4),
        "nValorCOFINS": (pyodbc.SQL_DECIMAL, 18, 4),
        "nValorCSLL": (pyodbc.SQL_DECIMAL, 18, 4),
        "nValorIR": (pyodbc.SQL_DECIMAL, 18, 4),
        "nValorISS": (pyodbc.SQL_DECIMAL, 18, 4),
        "nValorINSS": (pyodbc.SQL_DECIMAL, 18, 4),
        "nValorMovCC": (pyodbc.SQL_DECIMAL, 18, 4),
        "nValPago": (pyodbc.SQL_DECIMAL, 18, 4),
        "nValAberto": (pyodbc.SQL_DECIMAL, 18, 4),
        "nDesconto": (pyodbc.SQL_DECIMAL, 18, 4),
        "nJuros": (pyodbc.SQL_DECIMAL, 18, 4),
        "nMulta": (pyodbc.SQL_DECIMAL, 18, 4),
        "nValLiquido": (pyodbc.SQL_DECIMAL, 18, 4),

        # varchar fixo
        "cCodIntTitulo": (pyodbc.SQL_VARCHAR, 80),
        "cNumTitulo": (pyodbc.SQL_VARCHAR, 40),
        "cCPFCNPJCliente": (pyodbc.SQL_VARCHAR, 30),
        "cNumCtr": (pyodbc.SQL_VARCHAR, 30),
        "cNumOS": (pyodbc.SQL_VARCHAR, 20),
        "cStatus": (pyodbc.SQL_VARCHAR, 120),
        "cTipo": (pyodbc.SQL_VARCHAR, 20),
        "cOperacao": (pyodbc.SQL_VARCHAR, 10),
        "cNumDocFiscal": (pyodbc.SQL_VARCHAR, 40),
        "cCodCateg": (pyodbc.SQL_VARCHAR, 40),
        "cNumParcela": (pyodbc.SQL_VARCHAR, 15),
        "cCodigoBarras": (pyodbc.SQL_VARCHAR, 100),
        "cNSU": (pyodbc.SQL_VARCHAR, 120),
        "cNumBoleto": (pyodbc.SQL_VARCHAR, 50),
        "cChaveNFe": (pyodbc.SQL_VARCHAR, 60),
        "cOrigem": (pyodbc.SQL_VARCHAR, 10),
        "cGrupo": (pyodbc.SQL_VARCHAR, 40),

        # usuários (varchar 30 no stage)
        "cUsConcilia": (pyodbc.SQL_VARCHAR, 30),
        "cUsInc": (pyodbc.SQL_VARCHAR, 30),
        "cUsAlt": (pyodbc.SQL_VARCHAR, 30),

        # char(1)
        "cNatureza": (pyodbc.SQL_CHAR, 1),
        "cRetPIS": (pyodbc.SQL_CHAR, 1),
        "cRetCOFINS": (pyodbc.SQL_CHAR, 1),
        "cRetCSLL": (pyodbc.SQL_CHAR, 1),
        "cRetIR": (pyodbc.SQL_CHAR, 1),
        "cRetISS": (pyodbc.SQL_CHAR, 1),
        "cRetINSS": (pyodbc.SQL_CHAR, 1),
        "cLiquidado": (pyodbc.SQL_CHAR, 1),

        # varchar(max) -> bind com size seguro
        "observacao": (pyodbc.SQL_LONGVARCHAR, VARCHAR_BIND_MAX),

        # nvarchar(max) -> bind com size seguro
        "CategoriasJson": (pyodbc.SQL_WLONGVARCHAR, NVARCHAR_BIND_MAX),
        "DepartamentosJson": (pyodbc.SQL_WLONGVARCHAR, NVARCHAR_BIND_MAX),
        "MovimentoJson": (pyodbc.SQL_WLONGVARCHAR, NVARCHAR_BIND_MAX),

        # novas colunas (varchar)
        "TipoLancamento": (pyodbc.SQL_VARCHAR, 10),
        "EventKey": (pyodbc.SQL_VARCHAR, 200),
        "DocumentoKey": (pyodbc.SQL_VARCHAR, 200),
    }

    sizes = []
    for c in cols:
        t = TYPE_BY_COL.get(c)
        if t is None:
            raise RuntimeError(f"Coluna sem tipo no setinputsizes: {c}")
        sizes.append(t)

    cur.setinputsizes(sizes)

    def _debug_param_map(row: Tuple[Any, ...]) -> None:
        # imprime índice -> coluna -> tipo python -> valor resumido
        for idx, (col, val) in enumerate(zip(cols, row), start=1):
            tp = type(val).__name__
            if isinstance(val, str):
                vv = f"str(len={len(val)})"
            else:
                vv = repr(val)
            print(f"    [{idx:02d}] {col}: {tp} = {vv}")

    try:
        cur.executemany(insert_sql, valores)
    except Exception as e:
        # debug linha a linha
        for i, row in enumerate(valores):
            try:
                cur.execute(insert_sql, row)
            except Exception as e2:
                linha_debug = linhas_validas[i]
                print(">>> ERRO NA LINHA i =", i)
                print(">>> insert_sql:", insert_sql)
                print(">>> Chaves:", {
                    "IDEmpresaProprietaria": linha_debug.get("IDEmpresaProprietaria"),
                    "TipoLancamento": linha_debug.get("TipoLancamento"),
                    "EventKey": linha_debug.get("EventKey"),
                    "DocumentoKey": linha_debug.get("DocumentoKey"),
                    "nCodTitulo": linha_debug.get("nCodTitulo"),
                    "nCodBaixa": linha_debug.get("nCodBaixa"),
                    "nCodMovCC": linha_debug.get("nCodMovCC"),
                    "cGrupo": linha_debug.get("cGrupo"),
                })
                print(">>> MAPEAMENTO DE PARAMETROS (ordem do INSERT):")
                _debug_param_map(row)
                raise e2
        raise e
    finally:
        cur.close()

    # MERGE final (AGORA PELO EventKey)
    conn.execute(text("""
        MERGE dbo.MovimentacaoFinanceiro AS tgt
        USING #MovFinStage AS src
          ON tgt.IDEmpresaProprietaria = src.IDEmpresaProprietaria
         AND tgt.EventKey = src.EventKey
        WHEN MATCHED THEN
          UPDATE SET
            tgt.TipoLancamento = src.TipoLancamento,
            tgt.EventKey = src.EventKey,
            tgt.DocumentoKey = src.DocumentoKey,

            tgt.nPagina = src.nPagina,
            tgt.nTotPaginas = src.nTotPaginas,
            tgt.nRegistros = src.nRegistros,
            tgt.nTotRegistros = src.nTotRegistros,
            tgt.nCodTitulo = src.nCodTitulo,
            tgt.cCodIntTitulo = src.cCodIntTitulo,
            tgt.cNumTitulo = src.cNumTitulo,
            tgt.dDtEmissao = src.dDtEmissao,
            tgt.dDtVenc = src.dDtVenc,
            tgt.dDtPrevisao = src.dDtPrevisao,
            tgt.dDtPagamento = src.dDtPagamento,
            tgt.nCodCliente = src.nCodCliente,
            tgt.cCPFCNPJCliente = src.cCPFCNPJCliente,
            tgt.nCodCtr = src.nCodCtr,
            tgt.cNumCtr = src.cNumCtr,
            tgt.nCodOS = src.nCodOS,
            tgt.cNumOS = src.cNumOS,
            tgt.nCodCC = src.nCodCC,
            tgt.cStatus = src.cStatus,
            tgt.cNatureza = src.cNatureza,
            tgt.cTipo = src.cTipo,
            tgt.cOperacao = src.cOperacao,
            tgt.cNumDocFiscal = src.cNumDocFiscal,
            tgt.cCodCateg = src.cCodCateg,
            tgt.cNumParcela = src.cNumParcela,
            tgt.nValorTitulo = src.nValorTitulo,
            tgt.nValorPIS = src.nValorPIS,
            tgt.cRetPIS = src.cRetPIS,
            tgt.nValorCOFINS = src.nValorCOFINS,
            tgt.cRetCOFINS = src.cRetCOFINS,
            tgt.nValorCSLL = src.nValorCSLL,
            tgt.cRetCSLL = src.cRetCSLL,
            tgt.nValorIR = src.nValorIR,
            tgt.cRetIR = src.cRetIR,
            tgt.nValorISS = src.nValorISS,
            tgt.cRetISS = src.cRetISS,
            tgt.nValorINSS = src.nValorINSS,
            tgt.cRetINSS = src.cRetINSS,
            tgt.nCodProjeto = src.nCodProjeto,
            tgt.observacao = src.observacao,
            tgt.cCodVendedor = src.cCodVendedor,
            tgt.nCodComprador = src.nCodComprador,
            tgt.cCodigoBarras = src.cCodigoBarras,
            tgt.cNSU = src.cNSU,
            tgt.nCodNF = src.nCodNF,
            tgt.dDtRegistro = src.dDtRegistro,
            tgt.cNumBoleto = src.cNumBoleto,
            tgt.cChaveNFe = src.cChaveNFe,
            tgt.cOrigem = src.cOrigem,
            tgt.nCodTitRepet = src.nCodTitRepet,
            tgt.cGrupo = src.cGrupo,
            tgt.nCodMovCC = src.nCodMovCC,
            tgt.nValorMovCC = src.nValorMovCC,
            tgt.nCodMovCCRepet = src.nCodMovCCRepet,
            tgt.nCodBaixa = src.nCodBaixa,
            tgt.dDtCredito = src.dDtCredito,
            tgt.dDtConcilia = src.dDtConcilia,
            tgt.cHrConcilia = src.cHrConcilia,
            tgt.cUsConcilia = src.cUsConcilia,
            tgt.dDtInc = src.dDtInc,
            tgt.cHrInc = src.cHrInc,
            tgt.cUsInc = src.cUsInc,
            tgt.dDtAlt = src.dDtAlt,
            tgt.cHrAlt = src.cHrAlt,
            tgt.cUsAlt = src.cUsAlt,
            tgt.cLiquidado = src.cLiquidado,
            tgt.nValPago = src.nValPago,
            tgt.nValAberto = src.nValAberto,
            tgt.nDesconto = src.nDesconto,
            tgt.nJuros = src.nJuros,
            tgt.nMulta = src.nMulta,
            tgt.nValLiquido = src.nValLiquido,
            tgt.CategoriasJson = src.CategoriasJson,
            tgt.DepartamentosJson = src.DepartamentosJson,
            tgt.MovimentoJson = src.MovimentoJson,
            tgt.DataHoraCarga = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
          INSERT (
            IDEmpresaProprietaria,

            TipoLancamento,
            EventKey,
            DocumentoKey,

            nPagina, nTotPaginas, nRegistros, nTotRegistros,
            nCodTitulo, cCodIntTitulo, cNumTitulo,
            dDtEmissao, dDtVenc, dDtPrevisao, dDtPagamento,
            nCodCliente, cCPFCNPJCliente,
            nCodCtr, cNumCtr,
            nCodOS, cNumOS,
            nCodCC,
            cStatus, cNatureza, cTipo, cOperacao,
            cNumDocFiscal, cCodCateg,
            cNumParcela,
            nValorTitulo,
            nValorPIS, cRetPIS,
            nValorCOFINS, cRetCOFINS,
            nValorCSLL, cRetCSLL,
            nValorIR, cRetIR,
            nValorISS, cRetISS,
            nValorINSS, cRetINSS,
            nCodProjeto,
            observacao,
            cCodVendedor, nCodComprador,
            cCodigoBarras, cNSU,
            nCodNF, dDtRegistro,
            cNumBoleto, cChaveNFe,
            cOrigem,
            nCodTitRepet, cGrupo,
            nCodMovCC, nValorMovCC, nCodMovCCRepet,
            nCodBaixa,
            dDtCredito,
            dDtConcilia, cHrConcilia, cUsConcilia,
            dDtInc, cHrInc, cUsInc,
            dDtAlt, cHrAlt, cUsAlt,
            cLiquidado,
            nValPago, nValAberto, nDesconto, nJuros, nMulta, nValLiquido,
            CategoriasJson, DepartamentosJson,
            MovimentoJson,
            DataHoraCarga
          )
          VALUES (
            src.IDEmpresaProprietaria,

            src.TipoLancamento,
            src.EventKey,
            src.DocumentoKey,

            src.nPagina, src.nTotPaginas, src.nRegistros, src.nTotRegistros,
            src.nCodTitulo, src.cCodIntTitulo, src.cNumTitulo,
            src.dDtEmissao, src.dDtVenc, src.dDtPrevisao, src.dDtPagamento,
            src.nCodCliente, src.cCPFCNPJCliente,
            src.nCodCtr, src.cNumCtr,
            src.nCodOS, src.cNumOS,
            src.nCodCC,
            src.cStatus, src.cNatureza, src.cTipo, src.cOperacao,
            src.cNumDocFiscal, src.cCodCateg,
            src.cNumParcela,
            src.nValorTitulo,
            src.nValorPIS, src.cRetPIS,
            src.nValorCOFINS, src.cRetCOFINS,
            src.nValorCSLL, src.cRetCSLL,
            src.nValorIR, src.cRetIR,
            src.nValorISS, src.cRetISS,
            src.nValorINSS, src.cRetINSS,
            src.nCodProjeto,
            src.observacao,
            src.cCodVendedor, src.nCodComprador,
            src.cCodigoBarras, src.cNSU,
            src.nCodNF, src.dDtRegistro,
            src.cNumBoleto, src.cChaveNFe,
            src.cOrigem,
            src.nCodTitRepet, src.cGrupo,
            src.nCodMovCC, src.nValorMovCC, src.nCodMovCCRepet,
            src.nCodBaixa,
            src.dDtCredito,
            src.dDtConcilia, src.cHrConcilia, src.cUsConcilia,
            src.dDtInc, src.cHrInc, src.cUsInc,
            src.dDtAlt, src.cHrAlt, src.cUsAlt,
            src.cLiquidado,
            src.nValPago, src.nValAberto, src.nDesconto, src.nJuros, src.nMulta, src.nValLiquido,
            src.CategoriasJson, src.DepartamentosJson,
            src.MovimentoJson,
            SYSUTCDATETIME()
          );
    """))



def executar_com_retry_db(func, *args, **kwargs):
    for tentativa in range(1, MAX_TENTATIVAS_DB + 1):
        try:
            return func(*args, **kwargs)
        except Exception:
            if tentativa == MAX_TENTATIVAS_DB:
                raise
            time.sleep(1.5 * tentativa)



def _iterar_fatias(inicio: date, fim: date, janela_dias: int) -> List[Tuple[date, date]]:
    fatias: List[Tuple[date, date]] = []
    atual = inicio
    while atual <= fim:
        ate = min(fim, atual + timedelta(days=janela_dias - 1))
        fatias.append((atual, ate))
        atual = ate + timedelta(days=1)
    return fatias

def sincronizar_ambiente_por_tipo(ambiente: AmbienteOmie, tipo: str) -> None:
    hoje = date.today()

    with engine.begin() as conn:
        executar_com_retry_db(garantir_tabela_watermark, conn)
        ultimo_dt = obter_watermark(conn, ambiente.id_empresa_proprietaria, tipo)

    inicio_base = (ultimo_dt.date() - timedelta(days=OVERLAP_DIAS))
    fim_base = hoje

    fatias = _iterar_fatias(inicio_base, fim_base, JANELA_DIAS)

    maior_momento_global: Optional[datetime] = None
    total_processados = 0

    for (dt_de, dt_ate) in fatias:
        print(f"[RANGE] Empresa={ambiente.id_empresa_proprietaria} Tipo={tipo} AltDe={dt_de} AltAte={dt_ate}")
        pagina = 1
        maior_momento_fatia: Optional[datetime] = None

        while True:
            resp = omie_listar_movimentos(
                ambiente=ambiente,
                tipo_lancamento=tipo,
                pagina=pagina,
                reg_por_pagina=REGISTROS_POR_PAGINA,
                dt_alt_de=dt_de,
                dt_alt_ate=dt_ate,
            )

            n_tot_paginas = int(resp.get("nTotPaginas", 0) or 0)
            movimentos = resp.get("movimentos", []) or []
            qtd = len(movimentos)

            meta = {
                "nPagina": resp.get("nPagina"),
                "nTotPaginas": resp.get("nTotPaginas"),
                "nRegistros": resp.get("nRegistros"),
                "nTotRegistros": resp.get("nTotRegistros"),
            }

            print(f"  [PAGE] {pagina}/{n_tot_paginas or 0} movimentos={qtd}")

            linhas = []
            for mov in movimentos:
                linha = achatar_movimento(ambiente.id_empresa_proprietaria, meta, mov, tipo)
                linhas.append(linha)

                momento = extrair_momento_alteracao(linha)
                if momento is not None:
                    if maior_momento_fatia is None or momento > maior_momento_fatia:
                        maior_momento_fatia = momento

            def aplicar_pagina():
                with engine.begin() as tx_conn:
                    upsert_pagina(tx_conn, linhas)

            executar_com_retry_db(aplicar_pagina)

            total_processados += qtd

            if n_tot_paginas == 0 or pagina >= n_tot_paginas:
                break
            pagina += 1

        if maior_momento_fatia is not None:
            if maior_momento_global is None or maior_momento_fatia > maior_momento_global:
                maior_momento_global = maior_momento_fatia

    print(f"[DONE] Empresa={ambiente.id_empresa_proprietaria} Tipo={tipo} total_processados={total_processados}")

    if maior_momento_global:
        def aplicar_watermark():
            with engine.begin() as tx_conn:
                garantir_tabela_watermark(tx_conn)
                atualizar_watermark(tx_conn, ambiente.id_empresa_proprietaria, tipo, maior_momento_global)

        executar_com_retry_db(aplicar_watermark)
        print(f"[WATERMARK] atualizado para {maior_momento_global}")

def main() -> None:
    for ambiente in AMBIENTES:
        for tipo in TIPOS_LANCAMENTO:
            print(f"[SYNC] Empresa={ambiente.id_empresa_proprietaria} Tipo={tipo} ...")
            sincronizar_ambiente_por_tipo(ambiente, tipo)
            print(f"[OK]   Empresa={ambiente.id_empresa_proprietaria} Tipo={tipo}")

    print("Sincronização concluída.")

if __name__ == "__main__":
    main()
