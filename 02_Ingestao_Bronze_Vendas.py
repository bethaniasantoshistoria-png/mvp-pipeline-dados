# Databricks notebook source
df_vendas = spark.table("workspace.default.vendas")

display(df_vendas)

# COMMAND ----------

df_vendas.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_vendas")

# COMMAND ----------

df_bronze = spark.table("workspace.default.bronze_vendas")

display(df_bronze)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

total_registros = df_bronze.count()
total_unicos = df_bronze.dropDuplicates().count()
duplicados = total_registros - total_unicos

print("Total de registros:", total_registros)
print("Registros únicos:", total_unicos)
print("Registros duplicados:", duplicados)

# COMMAND ----------

from pyspark.sql.functions import col, sum, when

df_bronze.select([
    sum(
        when(
            col(c).isNull() | (col(c).cast("string") == ""),
            1
        ).otherwise(0)
    ).alias(c)
    for c in df_bronze.columns
]).display()

# COMMAND ----------

from pyspark.sql.functions import col, trim

df_silver = (
    df_bronze
    .dropDuplicates()
)

# Remove espaços extras das colunas de texto
for campo, tipo in df_silver.dtypes:
    if tipo == "string":
        df_silver = df_silver.withColumn(campo, trim(col(campo)))

print("Registros na Bronze:", df_bronze.count())
print("Registros na Silver:", df_silver.count())

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_vendas")

print("Tabela silver_vendas criada com sucesso!")

# COMMAND ----------

df_silver_validacao = spark.table("workspace.default.silver_vendas")

print("Total de registros:", df_silver_validacao.count())

display(df_silver_validacao)

# COMMAND ----------

from pyspark.sql.functions import sum, round

gold_produtos = (
    df_silver_validacao
    .groupBy("Categoria_Produto", "Produto")
    .agg(
        sum("Quantidade").alias("Volume_Vendido"),
        round(sum("Valor_Total"), 2).alias("Receita_Total")
    )
    .orderBy(col("Receita_Total").desc())
)

display(gold_produtos)

# COMMAND ----------

gold_produtos.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_vendas_produtos")

print("Tabela gold_vendas_produtos criada com sucesso!")

# COMMAND ----------

from pyspark.sql.functions import avg, sum, count, round, col

gold_regiao = (
    df_silver_validacao
    .groupBy("Regiao", "Cidade")
    .agg(
        count("ID_Venda").alias("Total_Pedidos"),
        round(sum("Valor_Total"), 2).alias("Receita_Total"),
        round(avg("Valor_Total"), 2).alias("Ticket_Medio")
    )
    .orderBy(col("Ticket_Medio").desc())
)

display(gold_regiao)

# COMMAND ----------

gold_regiao.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_vendas_regiao")

print("Tabela gold_vendas_regiao criada com sucesso!")

# COMMAND ----------

from pyspark.sql.functions import year, month, sum, count, round, col

gold_sazonalidade = (
    df_silver_validacao
    .groupBy(
        year("Data_Venda").alias("Ano"),
        month("Data_Venda").alias("Numero_Mes")
    )
    .agg(
        count("ID_Venda").alias("Total_Vendas"),
        sum("Quantidade").alias("Quantidade_Vendida"),
        round(sum("Valor_Total"), 2).alias("Receita_Total")
    )
    .orderBy("Ano", "Numero_Mes")
)

display(gold_sazonalidade)

# COMMAND ----------

gold_sazonalidade.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_vendas_sazonalidade")

print("Tabela gold_vendas_sazonalidade criada com sucesso!")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.default.gold_vendas_produtos
# MAGIC ALTER COLUMN Categoria_Produto COMMENT 'Categoria comercial à qual o produto pertence';
# MAGIC
# MAGIC ALTER TABLE workspace.default.gold_vendas_produtos
# MAGIC ALTER COLUMN Produto COMMENT 'Nome do produto vendido';
# MAGIC
# MAGIC ALTER TABLE workspace.default.gold_vendas_produtos
# MAGIC ALTER COLUMN Volume_Vendido COMMENT 'Quantidade total de unidades vendidas do produto';
# MAGIC
# MAGIC ALTER TABLE workspace.default.gold_vendas_produtos
# MAGIC ALTER COLUMN Receita_Total COMMENT 'Receita total obtida com as vendas do produto';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED workspace.default.gold_vendas_produtos;

# COMMAND ----------

from pyspark.sql.functions import col

# Verificações de consistência dos dados
quantidade_invalida = df_silver_validacao.filter(col("Quantidade") <= 0).count()

preco_invalido = df_silver_validacao.filter(col("Preco_Unitario") < 0).count()

valor_total_invalido = df_silver_validacao.filter(col("Valor_Total") < 0).count()

avaliacao_invalida = df_silver_validacao.filter(
    col("Avaliacao_Cliente").isNotNull() &
    ~col("Avaliacao_Cliente").between(1, 5)
).count()

print("Quantidade <= 0:", quantidade_invalida)
print("Preço unitário negativo:", preco_invalido)
print("Valor total negativo:", valor_total_invalido)
print("Avaliações fora do intervalo 1 a 5:", avaliacao_invalida)

# COMMAND ----------

registro_quantidade_invalida = df_silver_validacao.filter(
    col("Quantidade") <= 0
)

display(registro_quantidade_invalida)

# COMMAND ----------

registro_quantidade_invalida.select(
    "ID_Venda",
    "Data_Venda",
    "Categoria_Produto",
    "Produto",
    "Quantidade",
    "Preco_Unitario",
    "Valor_Bruto",
    "Valor_Total"
).display()

# COMMAND ----------

registro = registro_quantidade_invalida.select(
    "ID_Venda",
    "Produto",
    "Quantidade",
    "Preco_Unitario",
    "Valor_Bruto",
    "Valor_Total"
).first()

print("ID da Venda:", registro["ID_Venda"])
print("Produto:", registro["Produto"])
print("Quantidade:", registro["Quantidade"])
print("Preço Unitário:", registro["Preco_Unitario"])
print("Valor Bruto:", registro["Valor_Bruto"])
print("Valor Total:", registro["Valor_Total"])

# COMMAND ----------

# Remove registros com quantidade igual ou inferior a zero
df_silver_corrigido = df_silver.filter(
    col("Quantidade") > 0
)

print("Registros antes do tratamento:", df_silver.count())
print("Registros após o tratamento:", df_silver_corrigido.count())
print("Registros removidos:", df_silver.count() - df_silver_corrigido.count())

# COMMAND ----------

df_silver_corrigido.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_vendas")

print("Tabela Silver atualizada com sucesso!")
print("Total final de registros:", spark.table("workspace.default.silver_vendas").count())

# COMMAND ----------

from pyspark.sql.functions import col, sum, avg, count, round, year, month

# Carrega a Silver já corrigida
df_silver_final = spark.table("workspace.default.silver_vendas")


# GOLD 1 - Produtos
gold_produtos = (
    df_silver_final
    .groupBy("Categoria_Produto", "Produto")
    .agg(
        sum("Quantidade").alias("Volume_Vendido"),
        round(sum("Valor_Total"), 2).alias("Receita_Total")
    )
    .orderBy(col("Receita_Total").desc())
)

gold_produtos.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_vendas_produtos")


# GOLD 2 - Região e Cidade
gold_regiao = (
    df_silver_final
    .groupBy("Regiao", "Cidade")
    .agg(
        count("ID_Venda").alias("Total_Pedidos"),
        round(sum("Valor_Total"), 2).alias("Receita_Total"),
        round(avg("Valor_Total"), 2).alias("Ticket_Medio")
    )
    .orderBy(col("Ticket_Medio").desc())
)

gold_regiao.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_vendas_regiao")


# GOLD 3 - Sazonalidade
gold_sazonalidade = (
    df_silver_final
    .groupBy(
        year("Data_Venda").alias("Ano"),
        month("Data_Venda").alias("Numero_Mes")
    )
    .agg(
        count("ID_Venda").alias("Total_Vendas"),
        sum("Quantidade").alias("Quantidade_Vendida"),
        round(sum("Valor_Total"), 2).alias("Receita_Total")
    )
    .orderBy("Ano", "Numero_Mes")
)

gold_sazonalidade.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_vendas_sazonalidade")


print("Gold Produtos atualizada!")
print("Gold Região atualizada!")
print("Gold Sazonalidade atualizada!")

# COMMAND ----------

resultado_produtos = spark.table(
    "workspace.default.gold_vendas_produtos"
).orderBy(
    col("Receita_Total").desc()
)

display(resultado_produtos)

# COMMAND ----------

from pyspark.sql.functions import col

gold_produtos_final = spark.table(
    "workspace.default.gold_vendas_produtos"
)

print("=== PRODUTO COM MAIOR RECEITA ===")
gold_produtos_final \
    .orderBy(col("Receita_Total").desc()) \
    .show(1, truncate=False)

print("=== PRODUTO COM MAIOR VOLUME VENDIDO ===")
gold_produtos_final \
    .orderBy(col("Volume_Vendido").desc()) \
    .show(1, truncate=False)

# COMMAND ----------

top10_receita = (
    gold_produtos_final
    .orderBy(col("Receita_Total").desc())
    .limit(10)
)

display(top10_receita)

# COMMAND ----------

from pyspark.sql.functions import col

resultado_ticket = (
    spark.table("workspace.default.gold_vendas_regiao")
    .orderBy(col("Ticket_Medio").desc())
)

display(resultado_ticket)

# COMMAND ----------

maior_ticket = resultado_ticket.first()

print("=== MAIOR TICKET MÉDIO ===")
print("Região:", maior_ticket["Regiao"])
print("Cidade:", maior_ticket["Cidade"])
print("Total de pedidos:", maior_ticket["Total_Pedidos"])
print("Receita total: R$", maior_ticket["Receita_Total"])
print("Ticket médio: R$", maior_ticket["Ticket_Medio"])

# COMMAND ----------

top10_ticket = (
    resultado_ticket
    .limit(10)
)

display(top10_ticket)

# COMMAND ----------

from pyspark.sql.functions import col

sazonalidade = (
    spark.table("workspace.default.gold_vendas_sazonalidade")
    .orderBy("Ano", "Numero_Mes")
)

display(sazonalidade)

# COMMAND ----------

from pyspark.sql.functions import sum, avg, countDistinct, round

resumo_kpis = (
    spark.table("workspace.default.silver_vendas")
    .agg(
        countDistinct("ID_Venda").alias("Total_Vendas"),
        sum("Quantidade").alias("Quantidade_Vendida"),
        round(sum("Valor_Total"), 2).alias("Receita_Total"),
        round(avg("Valor_Total"), 2).alias("Ticket_Medio")
    )
)

display(resumo_kpis)