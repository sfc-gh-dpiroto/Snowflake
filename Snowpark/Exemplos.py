# =============================================================================
# Snowpark DataFrame - Guia Completo de Métodos com Dados Sintéticos
# =============================================================================

from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark import Window
from snowflake.snowpark.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, DateType, VariantType
)
import json

# ---------------------------------------------------------------------------
# Conexão
# ---------------------------------------------------------------------------
connection_params = {
    "account": "<sua_conta>",
    "user": "<seu_usuario>",
    "password": "<sua_senha>",
    "role": "ACCOUNTADMIN",
    "warehouse": "<seu_warehouse>",
    "database": "<seu_database>",
    "schema": "<seu_schema>",
}
session = Session.builder.configs(connection_params).create()

# =====================================================================
# 1. CRIAÇÃO DOS DATAFRAMES SINTÉTICOS
# =====================================================================

# -- Tabela de vendas
vendas_data = [
    ("V001", "Notebook",    "Eletronicos", 3500.00, 2,  "2024-01-15", "SP", True),
    ("V002", "Mouse",       "Eletronicos", 89.90,   10, "2024-01-15", "SP", True),
    ("V003", "Cadeira",     "Moveis",      1200.00, 1,  "2024-02-10", "RJ", True),
    ("V004", "Mesa",        "Moveis",      2500.00, 1,  "2024-02-10", "RJ", False),
    ("V005", "Teclado",     "Eletronicos", 250.00,  5,  "2024-03-05", "MG", True),
    ("V006", "Monitor",     "Eletronicos", 1800.00, 3,  "2024-03-05", "MG", True),
    ("V007", "Notebook",    "Eletronicos", 3500.00, 1,  "2024-04-20", "SP", True),
    ("V008", "Cadeira",     "Moveis",      1200.00, 2,  "2024-04-20", "RJ", True),
    ("V009", "Webcam",      "Eletronicos", None,    4,  "2024-05-01", "SP", False),
    ("V010", "Luminaria",   "Moveis",      350.00,  6,  "2024-05-01", "MG", True),
    ("V011", "Notebook",    "Eletronicos", 3500.00, 2,  "2024-01-15", "SP", True),  # duplicata intencional de V001
    ("V012", "Headset",     "Eletronicos", 450.00,  3,  "2024-06-15", "PR", True),
]

vendas_schema = StructType([
    StructField("VENDA_ID",   StringType()),
    StructField("PRODUTO",    StringType()),
    StructField("CATEGORIA",  StringType()),
    StructField("PRECO",      FloatType()),
    StructField("QUANTIDADE", IntegerType()),
    StructField("DATA_VENDA", StringType()),
    StructField("ESTADO",     StringType()),
    StructField("ENTREGUE",   BooleanType()),
])

df_vendas = session.create_dataframe(vendas_data, schema=vendas_schema)

# -- Tabela de clientes (para joins)
clientes_data = [
    ("V001", "Carlos Silva",    "carlos@email.com"),
    ("V002", "Maria Souza",     "maria@email.com"),
    ("V003", "João Lima",       "joao@email.com"),
    ("V004", "Ana Costa",       "ana@email.com"),
    ("V005", "Pedro Santos",    "pedro@email.com"),
    ("V006", "Lucia Ferreira",  "lucia@email.com"),
    ("V007", "Roberto Alves",   "roberto@email.com"),
    ("V008", "Fernanda Dias",   "fernanda@email.com"),
    ("V013", "Paulo Mendes",    "paulo@email.com"),  # sem venda correspondente
]

clientes_schema = StructType([
    StructField("VENDA_ID",      StringType()),
    StructField("NOME_CLIENTE",  StringType()),
    StructField("EMAIL",         StringType()),
])

df_clientes = session.create_dataframe(clientes_data, schema=clientes_schema)

# -- Tabela de metas por estado (para joins e comparações)
metas_data = [
    ("SP", 15000.00),
    ("RJ", 10000.00),
    ("MG", 8000.00),
    ("PR", 5000.00),
    ("BA", 3000.00),  # estado sem vendas
]

metas_schema = StructType([
    StructField("ESTADO",     StringType()),
    StructField("META_VALOR", FloatType()),
])

df_metas = session.create_dataframe(metas_data, schema=metas_schema)

# -- DataFrame com VARIANT (JSON aninhado)
json_data = [
    ("V001", '{"cor": "prata", "garantia_meses": 12, "tags": ["premium", "novo"]}'),
    ("V002", '{"cor": "preto", "garantia_meses": 6, "tags": ["basico"]}'),
    ("V003", '{"cor": "cinza", "garantia_meses": 24, "tags": ["ergonomico", "premium"]}'),
]

df_json = session.create_dataframe(json_data, schema=["VENDA_ID", "DETALHES_JSON"])


print("=" * 70)
print("DADOS SINTÉTICOS CRIADOS")
print("=" * 70)

print("\n--- df_vendas ---")
df_vendas.show()

print("\n--- df_clientes ---")
df_clientes.show()

print("\n--- df_metas ---")
df_metas.show()


# =====================================================================
# 2. SELEÇÃO E PROJEÇÃO
# =====================================================================
print("\n" + "=" * 70)
print("SELEÇÃO E PROJEÇÃO")
print("=" * 70)

# select: escolher colunas específicas
print("\n--- select(PRODUTO, PRECO, QUANTIDADE) ---")
df_vendas.select("PRODUTO", "PRECO", "QUANTIDADE").show()

# select com expressão calculada
print("\n--- select com coluna calculada (TOTAL = PRECO * QUANTIDADE) ---")
df_vendas.select(
    F.col("VENDA_ID"),
    F.col("PRODUTO"),
    (F.col("PRECO") * F.col("QUANTIDADE")).alias("TOTAL")
).show()

# drop: remover colunas
print("\n--- drop(ENTREGUE, DATA_VENDA) ---")
df_vendas.drop("ENTREGUE", "DATA_VENDA").show()

# with_column: adicionar nova coluna
print("\n--- with_column: adiciona TOTAL ---")
df_vendas.with_column(
    "TOTAL", F.col("PRECO") * F.col("QUANTIDADE")
).select("VENDA_ID", "PRODUTO", "PRECO", "QUANTIDADE", "TOTAL").show()

# with_column_renamed: renomear coluna
print("\n--- with_column_renamed: PRECO -> PRECO_UNITARIO ---")
df_vendas.with_column_renamed("PRECO", "PRECO_UNITARIO") \
    .select("VENDA_ID", "PRODUTO", "PRECO_UNITARIO").show()

# alias: dar alias ao DataFrame (útil em joins)
print("\n--- alias: df com alias 'v' e referência v.PRODUTO ---")
df_alias = df_vendas.alias("v")
df_alias.select(F.col("v", "PRODUTO"), F.col("v", "PRECO")).limit(3).show()

# col: referência direta a coluna
print("\n--- col: usando df.col() ---")
df_vendas.select(df_vendas.col("PRODUTO"), df_vendas.col("PRECO")).limit(3).show()


# =====================================================================
# 3. FILTRAGEM
# =====================================================================
print("\n" + "=" * 70)
print("FILTRAGEM")
print("=" * 70)

# filter: filtrar por condição
print("\n--- filter: PRECO > 1000 ---")
df_vendas.filter(F.col("PRECO") > 1000).show()

# where: sinônimo de filter
print("\n--- where: CATEGORIA == 'Moveis' AND ENTREGUE == True ---")
df_vendas.where(
    (F.col("CATEGORIA") == "Moveis") & (F.col("ENTREGUE") == True)
).show()

# distinct: valores únicos
print("\n--- distinct: combinações únicas de CATEGORIA + ESTADO ---")
df_vendas.select("CATEGORIA", "ESTADO").distinct().show()

# drop_duplicates: remover duplicatas por colunas
print("\n--- drop_duplicates: por PRODUTO, PRECO, QUANTIDADE, ESTADO ---")
df_vendas.drop_duplicates("PRODUTO", "PRECO", "QUANTIDADE", "ESTADO") \
    .select("VENDA_ID", "PRODUTO", "PRECO", "ESTADO").show()

# limit: primeiras N rows
print("\n--- limit(3) ---")
df_vendas.limit(3).show()


# =====================================================================
# 4. ORDENAÇÃO
# =====================================================================
print("\n" + "=" * 70)
print("ORDENAÇÃO")
print("=" * 70)

# sort: ordenar ascendente
print("\n--- sort: PRECO ascendente ---")
df_vendas.sort(F.col("PRECO").asc()).select("PRODUTO", "PRECO").show()

# order_by: ordenar descendente (sinônimo de sort)
print("\n--- order_by: QUANTIDADE desc, PRODUTO asc ---")
df_vendas.order_by(
    F.col("QUANTIDADE").desc(), F.col("PRODUTO").asc()
).select("PRODUTO", "QUANTIDADE").show()


# =====================================================================
# 5. JOINS
# =====================================================================
print("\n" + "=" * 70)
print("JOINS")
print("=" * 70)

# inner join
print("\n--- join INNER: vendas + clientes ---")
df_vendas.join(df_clientes, "VENDA_ID", "inner") \
    .select("VENDA_ID", "PRODUTO", "NOME_CLIENTE") \
    .sort("VENDA_ID").show()

# left join
print("\n--- join LEFT: vendas + clientes (mostra vendas sem cliente) ---")
df_vendas.join(df_clientes, "VENDA_ID", "left") \
    .select("VENDA_ID", "PRODUTO", "NOME_CLIENTE") \
    .sort("VENDA_ID").show()

# right join
print("\n--- join RIGHT: vendas + clientes (mostra clientes sem venda) ---")
df_vendas.join(df_clientes, "VENDA_ID", "right") \
    .select("VENDA_ID", "PRODUTO", "NOME_CLIENTE") \
    .sort("VENDA_ID").show()

# full outer join
print("\n--- join FULL: vendas + clientes ---")
df_vendas.join(df_clientes, "VENDA_ID", "full") \
    .select("VENDA_ID", "PRODUTO", "NOME_CLIENTE") \
    .sort("VENDA_ID").show()

# semi join: vendas que TEM cliente
print("\n--- join SEMI: vendas que possuem cliente ---")
df_vendas.join(df_clientes, "VENDA_ID", "semi") \
    .select("VENDA_ID", "PRODUTO").sort("VENDA_ID").show()

# anti join: vendas que NÃO tem cliente
print("\n--- join ANTI: vendas sem cliente ---")
df_vendas.join(df_clientes, "VENDA_ID", "anti") \
    .select("VENDA_ID", "PRODUTO").sort("VENDA_ID").show()

# cross join
print("\n--- cross_join: categorias x estados (amostra) ---")
df_categorias = df_vendas.select("CATEGORIA").distinct()
df_estados = df_vendas.select("ESTADO").distinct()
df_categorias.cross_join(df_estados).sort("CATEGORIA", "ESTADO").show()

# natural join
print("\n--- natural_join: vendas + clientes (por colunas de mesmo nome) ---")
df_vendas.natural_join(df_clientes) \
    .select("VENDA_ID", "PRODUTO", "NOME_CLIENTE") \
    .sort("VENDA_ID").limit(5).show()


# =====================================================================
# 6. AGREGAÇÃO
# =====================================================================
print("\n" + "=" * 70)
print("AGREGAÇÃO")
print("=" * 70)

# group_by + agg com múltiplas funções
print("\n--- group_by(CATEGORIA) + agg(sum, avg, count, min, max) ---")
df_vendas.group_by("CATEGORIA").agg(
    F.sum(F.col("PRECO") * F.col("QUANTIDADE")).alias("RECEITA_TOTAL"),
    F.avg("PRECO").alias("PRECO_MEDIO"),
    F.count("*").alias("QTD_VENDAS"),
    F.min("PRECO").alias("MENOR_PRECO"),
    F.max("PRECO").alias("MAIOR_PRECO"),
).sort("CATEGORIA").show()

# group_by com múltiplas colunas
print("\n--- group_by(CATEGORIA, ESTADO) ---")
df_vendas.group_by("CATEGORIA", "ESTADO").agg(
    F.sum("QUANTIDADE").alias("TOTAL_QTD"),
    F.count("*").alias("NUM_VENDAS"),
).sort("CATEGORIA", "ESTADO").show()

# rollup: subtotais hierárquicos
print("\n--- rollup(CATEGORIA, ESTADO) ---")
df_vendas.rollup("CATEGORIA", "ESTADO").agg(
    F.sum(F.col("PRECO") * F.col("QUANTIDADE")).alias("RECEITA")
).sort("CATEGORIA", "ESTADO").show()

# cube: todas as combinações de subtotais
print("\n--- cube(CATEGORIA, ESTADO) ---")
df_vendas.cube("CATEGORIA", "ESTADO").agg(
    F.sum(F.col("PRECO") * F.col("QUANTIDADE")).alias("RECEITA")
).sort("CATEGORIA", "ESTADO").show()

# pivot: rotacionar dados
print("\n--- pivot: receita por CATEGORIA, colunas = ESTADO ---")
df_vendas.with_column("TOTAL", F.col("PRECO") * F.col("QUANTIDADE")) \
    .pivot("ESTADO", ["SP", "RJ", "MG", "PR"]) \
    .agg(F.sum("TOTAL")) \
    .sort("CATEGORIA").show()

# unpivot: desfazer pivot
print("\n--- unpivot ---")
df_pivot = session.create_dataframe(
    [("Eletronicos", 8000, 3000, 2500), ("Moveis", 5000, 4900, 2100)],
    schema=["CATEGORIA", "SP", "RJ", "MG"]
)
df_pivot.unpivot("RECEITA", "ESTADO", ["SP", "RJ", "MG"]).show()


# =====================================================================
# 7. OPERAÇÕES DE CONJUNTO
# =====================================================================
print("\n" + "=" * 70)
print("OPERAÇÕES DE CONJUNTO")
print("=" * 70)

df_sp = df_vendas.filter(F.col("ESTADO") == "SP").select("PRODUTO").distinct()
df_mg = df_vendas.filter(F.col("ESTADO") == "MG").select("PRODUTO").distinct()

# union_all: combinar todos (com duplicatas)
print("\n--- union_all: produtos SP + produtos MG (com duplicatas) ---")
df_sp.union_all(df_mg).sort("PRODUTO").show()

# union: combinar sem duplicatas
print("\n--- union: produtos SP + produtos MG (sem duplicatas) ---")
df_sp.union(df_mg).sort("PRODUTO").show()

# intersect: produtos vendidos em SP E MG
print("\n--- intersect: produtos vendidos em SP E em MG ---")
df_sp.intersect(df_mg).sort("PRODUTO").show()

# except_: produtos vendidos em SP mas NÃO em MG
print("\n--- except_: produtos em SP mas NÃO em MG ---")
df_sp.except_(df_mg).sort("PRODUTO").show()


# =====================================================================
# 8. TRATAMENTO DE NULOS
# =====================================================================
print("\n" + "=" * 70)
print("TRATAMENTO DE NULOS")
print("=" * 70)

# dropna: remover rows com nulos
print("\n--- dropna: remove rows onde PRECO é null ---")
df_vendas.dropna(subset=["PRECO"]).select("VENDA_ID", "PRODUTO", "PRECO").show()

# fillna: preencher nulos
print("\n--- fillna: substitui PRECO null por 0.0 ---")
df_vendas.fillna({"PRECO": 0.0}).select("VENDA_ID", "PRODUTO", "PRECO").show()

# replace: substituir valores
print("\n--- replace: trocar estado 'SP' por 'SAO_PAULO' ---")
df_vendas.replace({"SP": "SAO_PAULO"}, subset=["ESTADO"]) \
    .select("VENDA_ID", "ESTADO").show()


# =====================================================================
# 9. WINDOW FUNCTIONS
# =====================================================================
print("\n" + "=" * 70)
print("WINDOW FUNCTIONS")
print("=" * 70)

# row_number
print("\n--- row_number: ranking por preço dentro de cada categoria ---")
w_cat = Window.partition_by("CATEGORIA").order_by(F.col("PRECO").desc())

df_vendas.filter(F.col("PRECO").is_not_null()) \
    .with_column("RANK", F.row_number().over(w_cat)) \
    .select("CATEGORIA", "PRODUTO", "PRECO", "RANK") \
    .sort("CATEGORIA", "RANK").show()

# rank e dense_rank
print("\n--- rank vs dense_rank por PRECO global ---")
w_global = Window.order_by(F.col("PRECO").desc())

df_vendas.filter(F.col("PRECO").is_not_null()) \
    .with_column("RANK", F.rank().over(w_global)) \
    .with_column("DENSE_RANK", F.dense_rank().over(w_global)) \
    .select("PRODUTO", "PRECO", "RANK", "DENSE_RANK") \
    .sort("RANK").show()

# lag e lead
print("\n--- lag/lead: venda anterior e próxima por data ---")
w_data = Window.order_by("DATA_VENDA")

df_vendas.filter(F.col("PRECO").is_not_null()) \
    .with_column("PRECO_ANTERIOR", F.lag("PRECO", 1).over(w_data)) \
    .with_column("PRECO_PROXIMO", F.lead("PRECO", 1).over(w_data)) \
    .select("DATA_VENDA", "PRODUTO", "PRECO", "PRECO_ANTERIOR", "PRECO_PROXIMO") \
    .sort("DATA_VENDA").show()

# sum acumulativo
print("\n--- sum acumulativo: receita acumulada por data ---")
w_cumsum = Window.order_by("DATA_VENDA").rows_between(
    Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW
)

df_vendas.filter(F.col("PRECO").is_not_null()) \
    .with_column("TOTAL", F.col("PRECO") * F.col("QUANTIDADE")) \
    .with_column("RECEITA_ACUM", F.sum("TOTAL").over(w_cumsum)) \
    .select("DATA_VENDA", "PRODUTO", "TOTAL", "RECEITA_ACUM") \
    .sort("DATA_VENDA").show()


# =====================================================================
# 10. FLATTEN (VARIANT / JSON)
# =====================================================================
print("\n" + "=" * 70)
print("FLATTEN (VARIANT / JSON)")
print("=" * 70)

# Primeiro, criar tabela temporária com VARIANT para usar flatten
df_json.write.save_as_table("TMP_JSON_VENDAS", mode="overwrite")
df_variant = session.table("TMP_JSON_VENDAS") \
    .with_column("DETALHES", F.parse_json("DETALHES_JSON"))

# Extrair campos do JSON
print("\n--- extrair campos do JSON ---")
df_variant.select(
    "VENDA_ID",
    F.col("DETALHES")["cor"].alias("COR"),
    F.col("DETALHES")["garantia_meses"].alias("GARANTIA_MESES"),
).show()

# flatten: explodir array do JSON
print("\n--- flatten: explodir array 'tags' do JSON ---")
df_variant.join_table_function(
    "flatten", F.col("DETALHES")["tags"]
).select(
    "VENDA_ID",
    F.col("VALUE").alias("TAG"),
).show()


# =====================================================================
# 11. SAMPLE
# =====================================================================
print("\n" + "=" * 70)
print("SAMPLE")
print("=" * 70)

print("\n--- sample: 50% das rows (probabilístico) ---")
df_vendas.sample(frac=0.5).select("VENDA_ID", "PRODUTO").show()


# =====================================================================
# 12. CACHE
# =====================================================================
print("\n" + "=" * 70)
print("CACHE")
print("=" * 70)

print("\n--- cache_result: materializa em tabela temporária ---")
df_caro = df_vendas.filter(F.col("PRECO") > 1000) \
    .with_column("TOTAL", F.col("PRECO") * F.col("QUANTIDADE"))

df_cached = df_caro.cache_result()

# Agora múltiplas operações reutilizam o resultado sem recalcular
print("Contagem:", df_cached.count())
df_cached.select("PRODUTO", "TOTAL").sort("TOTAL", ascending=False).show()


# =====================================================================
# 13. AÇÕES (executam a query)
# =====================================================================
print("\n" + "=" * 70)
print("AÇÕES")
print("=" * 70)

# collect
print("\n--- collect: retorna List[Row] ---")
rows = df_vendas.select("VENDA_ID", "PRODUTO").limit(3).collect()
for r in rows:
    print(f"  {r['VENDA_ID']} -> {r['PRODUTO']}")

# to_local_iterator
print("\n--- to_local_iterator: consome em chunks ---")
iterator = df_vendas.select("VENDA_ID", "PRODUTO").limit(3).to_local_iterator()
for r in iterator:
    print(f"  {r['VENDA_ID']} -> {r['PRODUTO']}")

# show
print("\n--- show(5) ---")
df_vendas.select("VENDA_ID", "PRODUTO", "PRECO").show(5)

# count
print("\n--- count ---")
print(f"Total de vendas: {df_vendas.count()}")

# first
print("\n--- first(3): retorna as 3 primeiras rows ---")
primeiras = df_vendas.first(3)
for r in primeiras:
    print(f"  {r['VENDA_ID']} - {r['PRODUTO']}")

# to_pandas
print("\n--- to_pandas: converte para pandas DataFrame ---")
pdf = df_vendas.select("PRODUTO", "PRECO", "QUANTIDADE").limit(5).to_pandas()
print(pdf)
print(f"Tipo: {type(pdf)}")

# describe
print("\n--- describe: estatísticas de PRECO e QUANTIDADE ---")
df_vendas.describe("PRECO", "QUANTIDADE").show()

# corr
print("\n--- corr: correlação entre PRECO e QUANTIDADE ---")
correlacao = df_vendas.corr("PRECO", "QUANTIDADE")
print(f"Correlação PRECO x QUANTIDADE: {correlacao}")

# cov
print("\n--- cov: covariância entre PRECO e QUANTIDADE ---")
covariancia = df_vendas.cov("PRECO", "QUANTIDADE")
print(f"Covariância PRECO x QUANTIDADE: {covariancia}")


# =====================================================================
# 14. DEBUG
# =====================================================================
print("\n" + "=" * 70)
print("DEBUG")
print("=" * 70)

# explain: mostra o SQL gerado
print("\n--- explain: SQL que será executado ---")
df_complexo = df_vendas \
    .filter(F.col("PRECO") > 500) \
    .group_by("CATEGORIA") \
    .agg(F.sum("PRECO").alias("TOTAL")) \
    .sort("TOTAL", ascending=False)
df_complexo.explain()

# queries: lista de queries
print("\n--- queries: lista de SQLs ---")
print(df_complexo.queries)

# schema: schema do DataFrame
print("\n--- schema ---")
print(df_vendas.schema)


# =====================================================================
# 15. PERSISTÊNCIA
# =====================================================================
print("\n" + "=" * 70)
print("PERSISTÊNCIA")
print("=" * 70)

# save_as_table
print("\n--- save_as_table: salva como tabela ---")
df_resumo = df_vendas.filter(F.col("PRECO").is_not_null()) \
    .group_by("CATEGORIA", "ESTADO") \
    .agg(
        F.sum(F.col("PRECO") * F.col("QUANTIDADE")).alias("RECEITA"),
        F.count("*").alias("NUM_VENDAS"),
    )
df_resumo.write.save_as_table("TMP_RESUMO_VENDAS", mode="overwrite")
print("Tabela TMP_RESUMO_VENDAS criada.")
session.table("TMP_RESUMO_VENDAS").show()

# create_or_replace_view
print("\n--- create_or_replace_view ---")
df_vendas.filter(F.col("CATEGORIA") == "Eletronicos") \
    .select("VENDA_ID", "PRODUTO", "PRECO") \
    .create_or_replace_view("TMP_VIEW_ELETRONICOS")
print("View TMP_VIEW_ELETRONICOS criada.")
session.table("TMP_VIEW_ELETRONICOS").show()

# create_or_replace_temp_view
print("\n--- create_or_replace_temp_view ---")
df_vendas.filter(F.col("ESTADO") == "SP") \
    .create_or_replace_temp_view("TMP_TEMP_VIEW_SP")
print("Temp view TMP_TEMP_VIEW_SP criada.")
session.table("TMP_TEMP_VIEW_SP").select("VENDA_ID", "PRODUTO", "ESTADO").show()


# =====================================================================
# 16. FUNÇÕES EXTRAS ÚTEIS
# =====================================================================
print("\n" + "=" * 70)
print("FUNÇÕES EXTRAS")
print("=" * 70)

# crosstab: tabela de contingência
print("\n--- crosstab: frequência CATEGORIA x ESTADO ---")
df_vendas.crosstab("CATEGORIA", "ESTADO").show()

# approx_quantile: percentis
print("\n--- approx_quantile: mediana e P90 de PRECO ---")
quantis = df_vendas.approx_quantile("PRECO", [0.5, 0.9])
print(f"Mediana (P50): {quantis[0]}, P90: {quantis[1]}")

# select com case/when
print("\n--- case/when: classificar preço ---")
df_vendas.select(
    "PRODUTO",
    "PRECO",
    F.when(F.col("PRECO").is_null(), "SEM_PRECO")
     .when(F.col("PRECO") < 300, "BARATO")
     .when(F.col("PRECO") < 1500, "MEDIO")
     .otherwise("CARO")
     .alias("FAIXA_PRECO")
).show()

# col_ilike: selecionar colunas por padrão
print("\n--- col_ilike: colunas que contêm 'PRE%' ---")
df_vendas.col_ilike("PRE%").show(3)


# =====================================================================
# LIMPEZA
# =====================================================================
print("\n" + "=" * 70)
print("LIMPEZA")
print("=" * 70)

session.sql("DROP TABLE IF EXISTS TMP_JSON_VENDAS").collect()
session.sql("DROP TABLE IF EXISTS TMP_RESUMO_VENDAS").collect()
session.sql("DROP VIEW IF EXISTS TMP_VIEW_ELETRONICOS").collect()
print("Objetos temporários removidos.")

session.close()
print("\nSessão encerrada.")
