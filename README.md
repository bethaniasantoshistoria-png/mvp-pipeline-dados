# MVP - Pipeline de Dados de Vendas na Nuvem

## 1. Contexto de Negócio e Objetivo

Este projeto foi desenvolvido como MVP da disciplina, com o objetivo de construir um pipeline de dados na nuvem utilizando o Databricks.

O cenário escolhido foi a análise de uma base de vendas, buscando transformar dados brutos em informações estruturadas e úteis para análise de negócio.

O pipeline foi desenvolvido seguindo a Arquitetura Medalhão, organizando os dados nas camadas Bronze, Silver e Gold.

### Objetivo

O objetivo principal é analisar o desempenho das vendas e identificar padrões relacionados a produtos, regiões, cidades e períodos.

### Perguntas de negócio

O projeto busca responder às seguintes perguntas:

1. Qual produto apresenta a maior receita?
2. Qual produto possui o maior volume de vendas?
3. Quais produtos apresentam as maiores receitas?
4. Quais cidades apresentam os maiores tickets médios?
5. Como a receita evolui ao longo dos meses?
6. Como a receita está distribuída entre as regiões?
7. Qual é a receita total, a quantidade total vendida, o número de vendas e o ticket médio da operação?

---

## 2. Busca pelos Dados

Para o desenvolvimento deste MVP foi utilizado o dataset **Vendas**, disponibilizado na plataforma Kaggle.

O conjunto de dados contém aproximadamente 1.500 registros de transações de vendas no varejo/e-commerce brasileiro, incluindo informações relacionadas a:

- Datas das vendas;
- Localização geográfica;
- Produtos e categorias;
- Valores financeiros;
- Descontos;
- Custos;
- Lucro e margem;
- Vendedores;
- Canais de venda;
- Meios de pagamento;
- Avaliações dos clientes.

### Fonte dos dados

**Plataforma:** Kaggle  
**Dataset:** Vendas  
**Arquivo utilizado:** `Vendas.csv`

### Licença

O dataset está disponibilizado sob a licença **CC0: Domínio Público**.

A utilização dos dados neste projeto possui finalidade acadêmica e está de acordo com a licença informada na página do conjunto de dados.

---

## 3. Coleta e Armazenamento dos Dados

A base de dados foi carregada para o ambiente Databricks, utilizado como plataforma de processamento e armazenamento na nuvem.

O processo inicial consistiu no upload do arquivo contendo os dados de vendas para o Databricks.

Após a ingestão, os dados foram armazenados em uma tabela da camada Bronze, preservando os dados provenientes da fonte para permitir rastreabilidade das etapas seguintes do pipeline.

### Camada Bronze

Tabela principal:

`bronze_vendas`

A camada Bronze representa os dados em seu estágio inicial de ingestão.

---

## 4. Modelagem e Catálogo de Dados

O projeto foi estruturado utilizando a Arquitetura Medalhão:

**Bronze → Silver → Gold**

### Bronze

A camada Bronze contém os dados provenientes da ingestão inicial.

Tabela:

`bronze_vendas`

### Silver

Na camada Silver os dados passaram por processos de limpeza, padronização e transformação para utilização nas análises.

Tabela:

`silver_vendas`

Entre os atributos utilizados nas análises estão informações como:

- Identificador da venda
- Data da venda
- Produto
- Categoria do produto
- Quantidade
- Preço unitário
- Valor da venda
- Custo
- Lucro
- Margem de lucro
- Região
- Cidade

### Gold

A camada Gold foi utilizada para disponibilizar dados agregados e preparados para responder às perguntas de negócio.

Foram criadas tabelas analíticas para diferentes perspectivas, incluindo:

`gold_vendas_produtos`

`gold_vendas_regiao`

`gold_vendas_sazonalidade`

Essas tabelas permitem analisar os dados sob perspectivas de produto, localização e comportamento das vendas ao longo do tempo.

---

## 5. Pipeline de Dados

O pipeline foi desenvolvido no Databricks seguindo o fluxo:

**Fonte de Dados → Bronze → Silver → Gold → Análises → Dashboard**

### Etapa 1 - Ingestão

Os dados foram carregados no Databricks e armazenados na camada Bronze.

### Etapa 2 - Transformação

A partir da camada Bronze foram realizadas transformações para gerar a camada Silver.

Nesta etapa, os dados foram preparados e estruturados para permitir consultas e análises de forma consistente.

### Etapa 3 - Agregações

A partir da camada Silver foram construídas tabelas Gold contendo agregações específicas para responder às perguntas de negócio.

### Etapa 4 - Visualização

Os dados tratados foram utilizados para construção de indicadores e gráficos em um dashboard no Databricks.

---

## 6. Qualidade dos Dados

Antes da realização das análises, os dados foram avaliados e tratados durante a construção da camada Silver.

Foram considerados aspectos relacionados à qualidade, como:

- Verificação de valores nulos;
- Verificação de registros duplicados;
- Consistência dos tipos de dados;
- Padronização dos dados;
- Validação das informações utilizadas nos cálculos e agregações.

O objetivo desta etapa foi garantir que os dados utilizados nas análises apresentassem consistência suficiente para responder às perguntas definidas no projeto.

---

## 7. Análise dos Dados

Após a construção do pipeline, foram realizadas análises sobre os dados tratados.

### Indicadores gerais

Os principais indicadores obtidos foram:

| Indicador | Resultado |
|---|---:|
| Receita Total | R$ 853.963,05 |
| Total de Vendas | 1.499 |
| Quantidade Vendida | 4.603 |
| Ticket Médio | R$ 569,69 |

### Produto com maior receita

O produto com maior receita foi:

**Smartphone X200**

- Quantidade vendida: 43
- Receita total: R$ 71.399,05

### Produto com maior volume vendido

O produto com maior volume de vendas foi:

**Pen Drive 64GB**

- Quantidade vendida: 377
- Receita total: R$ 13.676,51

### Ticket médio por cidade

A análise por cidade identificou **Belém** com o maior ticket médio observado na análise:

- Total de pedidos: 19
- Receita total: R$ 20.506,13
- Ticket médio: R$ 1.079,27

### Análise temporal

Foi realizada uma análise da evolução da receita ao longo dos meses, permitindo visualizar a sazonalidade e as variações do faturamento durante o período analisado.

### Análise regional

Também foi analisada a distribuição da receita entre as diferentes regiões, permitindo comparar o desempenho geográfico das vendas.

---

## 8. Dashboard

Foi desenvolvido um dashboard no Databricks para facilitar a visualização dos principais resultados do projeto.

O dashboard apresenta:

- Receita Total;
- Total de Vendas;
- Quantidade Vendida;
- Ticket Médio;
- Top 10 Produtos por Receita;
- Top 10 Cidades por Ticket Médio;
- Evolução da Receita por Mês;
- Receita por Região.

### Dashboard publicado

[Visualizar Dashboard no Databricks](https://dbc-15c01ce6-4166.cloud.databricks.com/dashboardsv3/01f1bad71b5f16e6b3043e809ace2b3f/published?o=7474659071119001)

---

## 9. Tecnologias Utilizadas

- Databricks Free Edition
- Apache Spark
- PySpark
- SQL
- Delta Tables
- GitHub
- Databricks Dashboards

---

## 10. Autoavaliação

O desenvolvimento deste MVP permitiu aplicar conceitos relacionados à Engenharia de Dados, desde a ingestão dos dados até sua disponibilização para análise.

A utilização da Arquitetura Medalhão permitiu organizar o pipeline em etapas distintas, separando os dados brutos dos dados tratados e dos dados preparados para consumo analítico.

Durante o desenvolvimento, foi possível compreender melhor a importância da qualidade dos dados, da organização das transformações e da construção de métricas capazes de responder às perguntas de negócio.

O dashboard desenvolvido possibilitou consolidar os principais indicadores e análises em uma única visualização, facilitando a interpretação dos resultados.

Como evolução futura do projeto, poderiam ser implementadas novas validações de qualidade, automação da execução do pipeline, inclusão de novos indicadores e ampliação das análises realizadas.

---

## 11. Estrutura do Projeto

O projeto foi organizado considerando as principais etapas do pipeline:

1. Ingestão dos dados;
2. Construção da camada Bronze;
3. Tratamento e construção da camada Silver;
4. Construção das tabelas Gold;
5. Análise dos dados;
6. Construção do dashboard;
7. Documentação dos resultados.

---

## Conclusão

O MVP demonstrou a construção de um pipeline de dados completo no Databricks, passando pelas etapas de ingestão, transformação, modelagem, análise e visualização.

A estrutura desenvolvida possibilitou transformar uma base de vendas em informações analíticas capazes de responder às perguntas de negócio definidas no início do projeto.
