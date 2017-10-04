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