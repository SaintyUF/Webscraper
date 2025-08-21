# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3


class WebScraperPipeline:
    def process_item(self, item, spider):
        return item


class DatabasePipeline:
    def open_spider(self, spider):
        # Conecta ao banco de dados (ou cria se não existir)
        self.connection = sqlite3.connect("precos.db")
        self.cursor = self.connection.cursor()
        # Cria a tabela
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS precos (
                produto_base TEXT,
                site TEXT,
                url TEXT,
                nome_encontrado TEXT,
                preco REAL,
                PRIMARY KEY (produto_base, site)
            )
        """
        )
        self.connection.commit()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        # Insere o item no banco de dados, atualizando se já existir
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO precos (produto_base, site, url, nome_encontrado, preco) 
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                item["produto_base"],
                item["site"],
                item["url"],
                item["nome_encontrado"],
                float(item["preco"]),
            ),
        )
        self.connection.commit()
        return item
