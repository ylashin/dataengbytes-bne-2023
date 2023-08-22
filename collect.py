# Databricks notebook source
df = spark.sql("SELECT ID, SHA2(CAST(ID AS STRING), 256) as VALUE FROM RANGE(20000000)")
df.show(5, False)
records = df.collect()
records[0]
