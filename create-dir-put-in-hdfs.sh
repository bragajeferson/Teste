#!/bin/bash

#Carregar os arquivos vendas.txt e vendas_itens.txt para a pasta tmp
hadoop fs -mkdir /user/mercafacil
hadoop fs -put /tmp/vendas.txt /user/mercafacil
hadoop fs -put /tmp/vendas_itens.txt /user/mercafacil
hadoop fs -ls /user