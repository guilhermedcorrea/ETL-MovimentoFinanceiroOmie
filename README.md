# Pipeline de Movimentos Financeiros — Omie → SQL Server

Integração desenvolvida em **Python** para consultar os movimentos financeiros da **API Omie**, tratar os dados e sincronizá-los com o **SQL Server**. A ideia é manter uma base financeira atualizada sem precisar buscar todo o histórico a cada execução.

O pipeline contempla **contas a pagar/receber, baixas e movimentos de conta corrente**. Os dados armazenados podem ser utilizados em consultas gerenciais, DRE, relatórios e dashboards.

> **Privacidade:** todos os identificadores, valores e registros apresentados neste README são fictícios. Não há credenciais, nomes de empresas, dados de clientes ou documentos reais.

## Visão geral

```text
API Omie — ListarMovimentos
           |
           v
Extração incremental por data de alteração
           |
           v
Paginação + tratamento de erros HTTP
           |
           v
Normalização dos campos e geração das chaves
           |
           v
Tabela temporária #MovFinStage (SQL Server)
           |
           v
MERGE → dbo.MovimentacaoFinanceiro
           |
           +----> dbo.MovFinWatermark (controle da sincronização)
           |
           v
Consultas SQL e regras gerenciais → DRE / BI
```

**Como classificar o processo?** A integração faz parte de um fluxo de **ETL**, porque o Python extrai e transforma campos antes de carregá-los. Existe também uma etapa de transformação **dentro do SQL Server**, responsável pelas classificações e cálculos gerenciais. Portanto, o processo completo combina características de ETL e ELT.

## Tecnologias

- **Python:** orquestração, requisições HTTP e tratamento dos registros.
- **Requests / urllib3:** comunicação com a API e política de tentativas em caso de falha.
- **SQLAlchemy + pyodbc:** conexão e gravação no SQL Server.
- **python-dotenv:** leitura de configurações e credenciais a partir do ambiente.
- **SQL Server:** armazenamento, `MERGE`, watermark e consultas gerenciais.

Documentação oficial: [Omie — Movimentos Financeiros](https://app.omie.com.br/api/v1/financas/mf/).

## 1. Extração da API

O método utilizado é **`ListarMovimentos`**, no endpoint:

```text
POST https://app.omie.com.br/api/v1/financas/mf/
```

O script executa a consulta separadamente por ambiente e pelos seguintes tipos de lançamento:

| Tipo | Conteúdo |
| --- | --- |
| `CPCR` | Lançamentos de contas a pagar e contas a receber |
| `BX` | Baixas dos lançamentos financeiros |
| `CC` | Movimentos de conta corrente |

A requisição usa `dDtAltDe` e `dDtAltAte` para buscar registros pela **data de alteração**, além de habilitar dados cadastrais (`lDadosCad`) e a distribuição por departamentos (`cExibirDepartamentos`).

**Exemplo de requisição (credenciais fictícias):**

```json
{
  "call": "ListarMovimentos",
  "app_key": "SUA_APP_KEY",
  "app_secret": "SEU_APP_SECRET",
  "param": [
    {
      "nPagina": 1,
      "nRegPorPagina": 50,
      "cTpLancamento": "CPCR",
      "lDadosCad": "S",
      "cExibirDepartamentos": "S",
      "dDtAltDe": "01/10/2026",
      "dDtAltAte": "15/10/2026"
    }
  ]
}
```

A API retorna os movimentos em `movimentos`, com informações em `detalhes`, `resumo`, `categorias` e `departamentos`. A resposta também informa a paginação (`nPagina`, `nTotPaginas`, `nRegistros` e `nTotRegistros`).

> **Atenção à paginação:** a documentação oficial apresenta um exemplo com `nRegPorPagina: 500`, mas informa **máximo de 50** na descrição do campo. O código analisado usa `REGISTROS_POR_PAGINA = 500`. Vale ajustar para `50` ou confirmar empiricamente qual limite a API está aceitando no ambiente.

## 2. Carga incremental e watermark

O objetivo do **watermark** é lembrar até que momento os dados foram sincronizados, evitando repetir uma extração completa em toda execução.

A tabela `dbo.MovFinWatermark` guarda uma linha para cada combinação de:

- `IDEmpresaProprietaria` — ambiente/empresa consultada;
- `TipoLancamento` — `CPCR`, `BX` ou `CC`;
- `UltimaDataHora` — maior data/hora de inclusão ou alteração identificada;
- `DataHoraAtualizacao` — momento da atualização do controle.

**Parâmetros configurados no script analisado:**

| Parâmetro | Valor | Finalidade |
| --- | --- | --- |
| `PRIMEIRA_CARGA_DESDE` | `2020-01-01` | Ponto inicial quando não existe watermark |
| `OVERLAP_DIAS` | `3` | Reconsulta dias anteriores ao último marco |
| `JANELA_DIAS` | `30` | Divide o período em intervalos menores |
| `REGISTROS_POR_PAGINA` | `500` | Tamanho de página configurado no script; conferir limite oficial |
| `MAX_TENTATIVAS_OMIE` | `5` | Tentativas adicionais da lógica de chamada à API |
| `MAX_TENTATIVAS_DB` | `5` | Tentativas de operações no banco |

**Fluxo:** o código lê o watermark, volta três dias, consulta a API em janelas de até 30 dias, processa todas as páginas e grava os registros. Após a conclusão do processamento do tipo de lançamento, atualiza o watermark com o maior momento de alteração/inclusão encontrado.

Se ainda não existir watermark, a sincronização parte da data inicial configurada, considerando a sobreposição aplicada pelo código.

**Importante:** o watermark não garante, por si só, a captura de qualquer alteração retroativa. A extração depende dos filtros de alteração informados pela API e da consistência de `dDtAlt`/`dDtInc`. A sobreposição reduz o risco de perda de mudanças próximas ao limite da consulta.

## 3. Tratamento e normalização

Cada item retornado pela API é convertido em uma linha da tabela `dbo.MovimentacaoFinanceiro`. A função `achatar_movimento()` reúne os objetos `detalhes` e `resumo`, preservando também a estrutura original em JSON.

O tratamento inclui:

- Conversão de datas no formato `DD/MM/AAAA` para objetos de data.
- Conversão de horas para o tipo apropriado.
- Conversão de identificadores para números inteiros.
- Conversão de valores financeiros para `DECIMAL(18,4)`.
- Preservação de `CategoriasJson`, `DepartamentosJson` e `MovimentoJson` para rastreabilidade.
- Criação das chaves técnicas `EventKey` e `DocumentoKey`.

### Exemplo simplificado de resposta da Omie

```json
{
  "detalhes": {
    "nCodTitulo": 987654,
    "cNumTitulo": "DOC-EXEMPLO-001",
    "cGrupo": "CONTA_A_PAGAR",
    "cNatureza": "P",
    "cCodCateg": "CAT-EXEMPLO",
    "nValorTitulo": 850.00,
    "dDtAlt": "10/10/2026",
    "cHrAlt": "14:20:00"
  },
  "resumo": {
    "cLiquidado": "S",
    "nValPago": 850.00,
    "nValAberto": 0.00
  }
}
```

Este JSON é **demonstrativo**, não uma transação real. Após a normalização, `nCodTitulo` é tratado como identificador inteiro, os valores financeiros como decimais e a data de alteração como data/hora utilizável no controle incremental.

## 4. EventKey: identificação do evento financeiro

A **`EventKey`** é uma chave técnica gerada pelo próprio pipeline para identificar uma ocorrência financeira e permitir o **upsert** (*insert ou update*). Ela **não é um campo nativo da API Omie**.

O código aplica esta ordem de prioridade:

```python
if nCodBaixa is not None:
    event_key = f"{id_empresa}|BX|{nCodBaixa}"
elif nCodMovCC is not None:
    event_key = f"{id_empresa}|CC|{nCodMovCC}"
elif nCodTitulo is not None:
    event_key = f"{id_empresa}|{tipo_api}|{grupo}|{nCodTitulo}"
else:
    event_key = None
```

| Identificador disponível | `EventKey` fictícia |
| --- | --- |
| `nCodBaixa = 123321` | `1\|BX\|123321` |
| `nCodMovCC = 765432`, sem baixa | `1\|CC\|765432` |
| `nCodTitulo = 987654`, sem baixa nem movimento CC, tipo `CPCR` | `1\|CPCR\|CONTA_A_PAGAR\|987654` |

Se nenhum desses três identificadores estiver disponível, a `EventKey` fica nula e o registro não é enviado ao `MERGE`.

**Por que isso importa?** Na próxima sincronização, o `MERGE` procura um registro com a mesma empresa e a mesma `EventKey`. Quando encontra, atualiza os campos; quando não encontra, insere um novo registro.

> A composição da `EventKey` precisa ser única **e estável** para o evento. Se a API representar a mesma ocorrência com identificadores diferentes em consultas distintas, pode haver duplicidade lógica; se eventos distintos produzirem a mesma chave, um poderá sobrescrever o outro. É importante validar esses casos com dados de teste.

## 5. DocumentoKey: vínculo com o título financeiro

A **`DocumentoKey`** é outra chave criada pelo pipeline. Seu objetivo é agrupar informações ligadas a um título financeiro quando elas compartilham **empresa, grupo e código do título**.

Sua composição atual é:

```python
documento_key = (
    f"{id_empresa}|{grupo}|TIT|{nCodTitulo}"
    if nCodTitulo is not None else None
)
```

Exemplo com o título fictício `987654`:

| Tipo de registro | `cGrupo` | `DocumentoKey` |
| --- | --- | --- |
| `CPCR` | `CONTA_A_PAGAR` | `1\|CONTA_A_PAGAR\|TIT\|987654` |
| `BX` | `CONTA_CORRENTE_PAG` | `1\|CONTA_CORRENTE_PAG\|TIT\|987654` |

**Ponto importante do código atual:** embora os dois registros tenham o mesmo `nCodTitulo`, eles geram **`DocumentoKey` diferentes**, porque o `cGrupo` faz parte da chave. Portanto, ela **não unifica automaticamente todos os eventos do mesmo título entre grupos distintos**. Caso essa seja a regra desejada, será necessário revisar a composição e validar a unicidade de `nCodTitulo` por empresa.

### EventKey x DocumentoKey

| Chave | O que representa | Uso principal |
| --- | --- | --- |
| `EventKey` | A ocorrência financeira, conforme a prioridade de identificadores | Localizar a linha correta no `MERGE` e controlar duplicações |
| `DocumentoKey` | O título *dentro de um grupo* | Relacionar linhas associadas ao mesmo título e grupo |
| `IdChaveOmie` | Identificador auxiliar calculado no banco | Usar o primeiro valor disponível de `nCodBaixa`, `nCodMovCC` ou `nCodTitulo` |

O campo auxiliar `IdChaveOmie` utiliza a expressão SQL:

```sql
COALESCE(nCodBaixa, nCodMovCC, nCodTitulo)
```

## 6. Persistência no SQL Server

Para cada página de registros, a função `upsert_pagina()`:

1. Descarta linhas sem `EventKey`.
2. Cria a tabela temporária `#MovFinStage`.
3. Insere as linhas de forma agrupada com `pyodbc` e `fast_executemany`.
4. Executa `MERGE` na tabela `dbo.MovimentacaoFinanceiro`.
5. Atualiza `DataHoraCarga` para os registros processados.

**Condição utilizada pelo `MERGE`:**

```sql
ON tgt.IDEmpresaProprietaria = src.IDEmpresaProprietaria
AND tgt.EventKey = src.EventKey
```

Dessa forma, reprocessar uma janela de datas não precisa gerar novas linhas para eventos que continuem com a mesma chave. A atualização e a inclusão são realizadas por página em uma transação do banco.

O campo `MovimentoJson` preserva o movimento completo retornado pela API. Já `CategoriasJson` e `DepartamentosJson` guardam as distribuições, quando enviadas.

## 7. Regras gerenciais e DRE

Além da ingestão, existe uma **consulta SQL gerencial** que cruza os movimentos com tabelas auxiliares:

| Tabela | Papel na consulta |
| --- | --- |
| `MovimentacaoFinanceiro` (`MVF`) | Fatos financeiros extraídos da API |
| `CategoriasCadastro` (`CTO`) | Descrições e classificação original das categorias |
| `PlanoCategoriasGerencial` (`PCG`) | Mapeamento gerencial por empresa e categoria |
| `FomatoDRE` (`FDRE`) | Ordem, grupos, linhas e subtotais da DRE |

A consulta resolve o campo `CampoDRE_Final` por prioridade: primeiro utiliza `PCG.Nivel1`, se estiver preenchido; caso contrário, aplica regras de classificação sobre `CTO.dadosDRE_descricaoDRE`; quando nenhuma regra se aplica, classifica como **`Não classificado`**.

Ela também calcula:

| Campo calculado | Regra do SQL atual |
| --- | --- |
| `Flag_Realizado` | `1` quando `cLiquidado = 'S'`; caso contrário, `0` |
| `Flag_EmAberto` | `1` quando `nValAberto > 0`; caso contrário, `0` |
| `Sinal` | `-1` para pagamento/despesa e `+1` para recebimento/receita, conforme `cGrupo` e `cNatureza` |
| `ValorPago_Assinado` | `nValPago * Sinal` |
| `ValorDRE_Realizado` | `nValPago * Sinal` somente quando liquidado e com sinal identificado; senão `0` |
| `ValorDRE_Previsto` | `nValAberto * Sinal` quando há saldo em aberto e sinal identificado; senão `0` |

Um recorte **simplificado** da lógica de classificação é:

```sql
CASE
    WHEN NULLIF(LTRIM(RTRIM(PCG.Nivel1)), '') IS NOT NULL
        THEN LTRIM(RTRIM(PCG.Nivel1))
    WHEN UPPER(LTRIM(RTRIM(CTO.dadosDRE_descricaoDRE))) = 'RECEITA BRUTA'
        THEN 'Receita Bruta'
    ELSE 'Não classificado'
END AS CampoDRE_Final
```

As regras completas para custos, despesas, impostos, resultado financeiro e outros resultados permanecem na consulta SQL do projeto.

**Cuidados com a análise:**

- O campo chamado `ValorDRE_Realizado` é calculado a partir de **pagamentos/liquidações**, não constitui automaticamente uma DRE contábil por competência.
- É preciso validar se os tipos `CPCR`, `BX` e `CC` representam fatos sobrepostos antes de agregá-los na mesma medida, evitando dupla contagem.
- Na consulta fornecida, `YEAR(MVF.dDtPagamento) > 2022` elimina registros com `dDtPagamento` nula, o que pode retirar justamente títulos em aberto da análise de `ValorDRE_Previsto`. Para projeções, deve ser considerada a data adequada, como previsão ou vencimento, conforme a regra de negócio.
- Para desempenho, filtros diretos por intervalo de datas costumam ser melhores do que aplicar `YEAR()` à coluna filtrada.
- Os relacionamentos (`JOIN`) precisam garantir cardinalidade adequada; múltiplos mapeamentos de categoria ou grupo podem multiplicar movimentos e valores.

## 8. Configuração local

Instale as dependências Python e o driver ODBC compatível com o SQL Server:

```bash
pip install requests sqlalchemy pyodbc python-dotenv urllib3
```

Crie um arquivo `.env` **fora do controle de versão** com as variáveis exigidas pela configuração de ambientes no script. Exemplo **genérico**, sem dados reais:

```dotenv
# Exemplo: adapte os nomes ao mapeamento AMBIENTES do seu código.
OMIE_APP_KEY_EMPRESA_1=SUA_APP_KEY
OMIE_APP_SECRET_EMPRESA_1=SEU_APP_SECRET
OMIE_APP_KEY_EMPRESA_2=SUA_APP_KEY
OMIE_APP_SECRET_EMPRESA_2=SEU_APP_SECRET

SERVER=SEU_SERVIDOR_SQL
DATABASE=SEU_BANCO
UID=SEU_USUARIO
PWD=SUA_SENHA
```

> **Atenção:** o exemplo acima usa nomes públicos e genéricos. O código analisado ainda referencia nomes específicos de variáveis para cada ambiente. Ajuste `AMBIENTES` para os nomes acima, ou mantenha localmente o mapeamento já existente. Nunca publique o arquivo `.env` nem segredos em logs, commits ou documentação.

Inclua pelo menos estas entradas no `.gitignore`:

```gitignore
.env
.env.*
!.env.example
__pycache__/
*.pyc
.venv/
```

O script espera que a tabela de destino `dbo.MovimentacaoFinanceiro` já exista com as colunas correspondentes. A tabela `dbo.MovFinWatermark`, por sua vez, é criada automaticamente se estiver ausente.

Para executar, substitua pelo nome real do arquivo Python:

```bash
python nome_do_script.py
```

A execução percorre os ambientes configurados e, em cada um, processa `CPCR`, `BX` e `CC`.

## 9. Pontos de atenção antes de colocar em produção

1. **Paginação:** conferir o limite de registros por página aceito pela Omie; a documentação consultada indica máximo de 50, apesar de apresentar um exemplo com 500.
2. **Chaves:** testar cenários de baixa, conta corrente e título para confirmar estabilidade/unicidade da `EventKey` e a finalidade da `DocumentoKey`.
3. **Incremental:** monitorar a diferença entre registros recebidos, gravados e descartados por falta de chave. Falhas e reprocessamentos merecem logs estruturados.
4. **DRE:** separar movimentos de título e de caixa quando representarem o mesmo fato econômico; definir claramente regime de caixa x competência.
5. **Datas e fuso:** `DataHoraCarga` e `DataHoraAtualizacao` usam `SYSUTCDATETIME()`; horários da API e do SQL precisam de tratamento consistente.
6. **Segurança:** não versionar credenciais nem registros reais; restringir acesso a colunas de documento de cliente e ao JSON original.
7. **Validação:** criar restrição ou índice único apropriado para a identidade do evento após confirmar a unicidade real e verificar duplicatas no staging antes do `MERGE`.

## Referência

- [Documentação oficial Omie — API de Movimentos Financeiros / `ListarMovimentos`](https://app.omie.com.br/api/v1/financas/mf/)

---

**Em resumo:** o pipeline consulta alterações na Omie, normaliza os movimentos, identifica eventos com `EventKey`, relaciona títulos e grupos com `DocumentoKey`, faz `MERGE` no SQL Server e registra o último ponto de sincronização. Depois, consultas SQL aplicam regras de classificação financeira para alimentar análises e relatórios.
