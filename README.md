# Teste #

Carregar os dados de vendas para dentro do HDFS e através do Spark gerar sumarização dos dados e gravação dos resultados no MongoDB

### Pré-requisitos ###
   **Ambiente**
   - CentOS 6.7
   - Spark 1.6
   - MongoDB 3.4
   - Spark-mongo 1.10
   - Scala 2.10
### Carregamento dos arquivos CSV para o HDFS ###
**1.	Usando o Ambari para carregar**

![WAVFRM_SAMPLE](https://github.com/bragajeferson/Teste/blob/master/Ambari-HDFS.png)

**2.	Via linha de comando**

```sh
    hadoop fs -mkdir /user/mercafacil
  
    hadoop fs -put /tmp/vendas.txt /user/mercafacil
  
    hadoop fs -put /tmp/vendas_itens.txt /user/mercafacil
  
    hadoop fs -ls /user
```

### Leitura dos arquivos CSV do HDFS para o Spark usando Zeppelin ###

**1.	Criado os SQLContext para carregar os arquivos para o Spark**

![WAVFRM_SAMPLE](https://github.com/bragajeferson/Teste/blob/master/Zepplelin-SQLContext1.png)

![WAVFRM_SAMPLE](https://github.com/bragajeferson/Teste/blob/master/Zepplelin-SQLContext2.png)


**2.	Construído a estrutura dos DataFrames e armazenado em tabelas temporárias para futura consulta**

![WAVFRM_SAMPLE](https://github.com/bragajeferson/Teste/blob/master/Zeppelin-DataFrame.png)

**3.	Criado as consultas em SQL para validar os resultados**

![WAVFRM_SAMPLE](https://github.com/bragajeferson/Teste/blob/master/Zeppelin-Query1.png)
![WAVFRM_SAMPLE](https://github.com/bragajeferson/Teste/blob/master/Zeppelin-Query2.png)
![WAVFRM_SAMPLE](https://github.com/bragajeferson/Teste/blob/master/Zeppelin-Query3.png)

### Instalado o MongoDB ###
**1.	Instalação do MongoDB**

```sh
    vi /etc/yum.repos.d/mongodb-org-3.4.repo
    
    [mongodb-org-3.4]
    name=MongoDB Repository
    baseurl=https://repo.mongodb.org/yum/redhat/6/mongodb-org/3.4/x86_64/
    enabled=1
    gpgkey=https://www.mongodb.org/static/pgp/server-3.4.asc
    
    mkdir /data/db
    
    service mongod start
```

**2.	Instancia do mongod escutando na porta 27017**

![WAVFRM_SAMPLE](https://github.com/bragajeferson/Teste/blob/master/Instancia-MongoDB.png)

### Scala ###

**1.	Habilitar o Spark acessar o MongoDB usando o Scala**

```sh
    ./bin/spark-shell –conf "spark.mongodb.output.uri=mongodb://127.0.0.1/results.collection" --packages org.mongodb.spark:mongo-spark-connector_2.10:1.1.0
```
    
**2.	Executando código Scala para verificar o conteúdo do DataFrame antes de gravar no MongoDB**

![WAVFRM_SAMPLE](https://github.com/bragajeferson/Teste/blob/master/Scala-DataFrame.png)
![WAVFRM_SAMPLE](https://github.com/bragajeferson/Teste/blob/master/Zeppelin-DataFrame2.png)

**3.	Gravação dos dados no MongoDB**

```scala
    vendas_por_cliente.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://127.0.0.1/mercafacil.vendasporcliente")))
    vendas_por_dia.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://127.0.0.1/mercafacil.vendaspordia")))
    vendas_por_produto.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://127.0.0.1/mercafacil.vendasporproduto")))
```

### Scripts Scala ###

**1.	Tabela Vendas**

```scala
    import sqlContext.implicits._
    import com.mongodb.spark.config._
    import com.mongodb.spark._
    import com.mongodb.spark.sql._

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val bankText = sc.textFile("/user/mercafacil/vendas.txt")
    case class Vendas(id_loja: Integer, id_venda: Integer, numero_caixa: Integer, data_venda: String, hora_venda: String, valor_total_sem_desc: Float, valor_desconto: Float, valor_total_com_desc: Float, id_cliente_1: Integer, id_cliente_2: Integer)

    val vendas = bankText.map(s => s.split(";")).map(
        s => Vendas(s(0).toInt, 
                s(1).toInt,
                s(2).toInt,
                s(3).toString,
                s(4).toString,
                s(5).toFloat,
                s(6).toFloat,
                s(7).toFloat,
                s(8).toInt,
                s(9).toInt
            )
    )
    
    vendas.toDF().registerTempTable("vendas")
    
    --Total de vendas cliente
    --%sql
    --select id_cliente_1, round(sum(valor_total_sem_desc),2) as valor_total_sem_desc, round(sum(valor_total_com_desc),2) as --valor_total_com_desc  from vendas GROUP BY id_cliente_1 ORDER BY id_cliente_1

    val vendas_por_cliente = sqlContext.sql("select id_cliente_1, translate(round(sum(valor_total_sem_desc),2),'.',',') as valor_total_sem_desc, translate(round(sum(valor_total_com_desc),2),'.',',') as valor_total_com_desc  from vendas GROUP BY id_cliente_1 ORDER BY id_cliente_1")

    --Total de vendas por dia
    --%sql
    --select DAY(CAST(UNIX_TIMESTAMP(data_venda, 'dd/MM/yyyy') AS TIMESTAMP)) as dia, round(sum(valor_total_sem_desc),2) as --valor_total_sem_desc,  round(sum(valor_total_com_desc),2) as valor_total_com_desc from vendas group by DAY(CAST(UNIX_TIMESTAMP(data_venda, --'dd/MM/yyyy') AS TIMESTAMP))

    val vendas_por_dia = sqlContext.sql("select DAY(CAST(UNIX_TIMESTAMP(data_venda, 'dd/MM/yyyy') AS TIMESTAMP)) as dia, translate(round(sum(valor_total_sem_desc),2),'.',',') as valor_total_sem_desc,  translate(round(sum(valor_total_com_desc),2),'.',',') as valor_total_com_desc from vendas group by DAY(CAST(UNIX_TIMESTAMP(data_venda, 'dd/MM/yyyy') AS TIMESTAMP))")

    vendas_por_cliente.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://127.0.0.1/mercafacil.vendasporcliente")))
    vendas_por_dia.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://127.0.0.1/mercafacil.vendaspordia")))
```

**2.	Tabela vendas_itens**

```scala
    import sqlContext.implicits._
    import com.mongodb.spark.config._
    import com.mongodb.spark._
    import com.mongodb.spark.sql._

    val vendasitens = sc.textFile("/user/mercafacil/vendas_itens.txt")

    case class Vendas_itens(id_loja: Integer, id_venda: Integer, numero_caixa: Integer, id_produto: Integer, quantidade: Float, valor_unitario: Float, valor_total_sem_desc: Float, valor_desconto: Float, valor_total_com_desc: Float, id_profissional_1: Integer, id_profissional_2: Integer)

    val vendas_itens = vendasitens.map(s => s.split(";")).map(
    s => Vendas_itens(s(0).toInt, 
            s(1).toInt,
            s(2).toInt,
            s(3).toInt,
            s(4).replaceAll(",",".").toFloat,
            s(5).toFloat,
            s(6).toFloat,
            s(7).toFloat,
            s(8).toFloat,
            s(9).toInt,
			s(10).toInt
        )
    )

    vendas_itens.toDF().registerTempTable("Vendas_itens")

    --Total de vendas por produto
    --%sql
    --select Vendas_itens.id_produto, translate(round(sum(Vendas_itens.valor_total_sem_desc), 3), '.', ',') as valor_total_sem_desc, --translate(round(sum(Vendas_itens.valor_total_com_desc), 3), '.', ',') as valor_total_com_desc from Vendas_itens group by --Vendas_itens.id_produto order by 1

    val vendas_por_produto = sqlContext.sql("select Vendas_itens.id_produto, translate(round(sum(Vendas_itens.valor_total_sem_desc), 3), '.', ',') as valor_total_sem_desc, translate(round(sum(Vendas_itens.valor_total_com_desc), 3), '.', ',') as valor_total_com_desc from Vendas_itens group by Vendas_itens.id_produto order by 1")

    vendas_por_produto.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://127.0.0.1/mercafacil.vendasporproduto")))
```
