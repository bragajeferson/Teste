val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val bankText = sc.textFile("/user/mercafacil/vendas.txt")

case class Vendas(id_loja: Integer, id_venda: Integer, numero_caixa: Integer, data_venda: String, hora_venda: String, valor_total_sem_desc: Float, valor_desconto: Float, valor_total_com_desc: Float, id_cliente_1: Integer, id_cliente_2: Integer)

val vendas = bankText.flatMap(_.split(";"))

-- Fazendo o mapping
val bank = bankText.map(s => s.split(";")).map(
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

--Guardar como tabela temporaria
vendas.toDF().registerTempTable("vendas")

--Verificar se a tabela temporaria criada a partir do RDD foi carregada
%sql
select * from vendas

--Total de vendas cliente
%sql
select id_cliente_1,
        round(sum(valor_total_sem_desc),2) as valor_total_sem_desc, 
        round(sum(valor_total_com_desc),2) as valor_total_com_desc 
from vendas
GROUP BY id_cliente_1
ORDER BY id_cliente_1

--Total de vendas por dia
%sql
select 
DAY(CAST(UNIX_TIMESTAMP(data_venda, 'dd/MM/yyyy') AS TIMESTAMP)) as dia,
round(sum(valor_total_sem_desc),2) as valor_total_sem_desc, 
        round(sum(valor_total_com_desc),2) as valor_total_com_desc 
from vendas
group by DAY(CAST(UNIX_TIMESTAMP(data_venda, 'dd/MM/yyyy') AS TIMESTAMP))