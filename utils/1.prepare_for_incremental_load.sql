-- Databricks notebook source
drop database if exists hf1_processed cascade;

-- COMMAND ----------

create database if not exists hf1_processed
location "/mnt/btc1dl/harrisdlprocessed"

-- COMMAND ----------

drop database if exists hf1_presentation cascade;

-- COMMAND ----------

create database if not exists hf1_presentation
location "/mnt/btc1dl/harrisdlpresentation"