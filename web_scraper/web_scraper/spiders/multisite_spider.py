import scrapy
import pandas as pd
import urllib.parse
import unidecode

class MultiSiteSpider(scrapy.Spider):
    name = 'multisite_spider'
    handle_httpstatus_list = [404]

    # Roda este spider com: scrapy crawl multisite_spider

    def start_requests(self):
        # --- Configuração dos Sites ---
        sites_config = {
            'ferreiracosta': {
                'base_url': 'https://www.ferreiracosta.com/pesquisa/',
                'container': 'section[data-testid="product-card-content"]',
                'name': 'h3[data-testid="product-card-title"]::text',
                'price': 'span[data-testid="product-card-current-price"]::text',
            },
            'nortel': {
                'base_url': 'https://express.nortel.com.br/catalogsearch/result/?q=',
                'container': 'div.vtex-search-result-3-x-galleryItem',
                'name': 'span.vtex-product-summary-2-x-productBrand::text',
                'price': 'span.vtex-product-summary-2-x-currencyContainer',
            }
        }

        # --- Leitura da Planilha ---
        # Coloque o arquivo 'Relatorio_de_Materiais.xlsx' na pasta raiz do projeto
        try:
            df_produtos = pd.read_excel('Relatorio_de_Materiais.xlsx')
            produtos = df_produtos['Produto'].dropna().tolist()
        except FileNotFoundError:
            self.logger.error("Arquivo 'Relatorio_de_Materiais.xlsx' não encontrado!")
            return
        except KeyError:
            self.logger.error("A coluna 'Produto' não foi encontrada no arquivo Excel!")
            return

        # --- Geração das Requisições ---
        for site, config in sites_config.items():
            for produto in produtos:
                termo_limpo = self.limpar_termo_busca(produto)
                termo_busca = urllib.parse.quote(termo_limpo)
                url = config['base_url'] + termo_busca
                yield scrapy.Request(
                    url,
                    callback=self.parse,
                    meta={
                        'config': config,
                        'produto_base': produto,
                        'site': site
                    }
                )

    def parse(self, response):
        config = response.meta['config']
        produto_base = response.meta['produto_base']
        site = response.meta['site']

        produto_base_normalizado = unidecode.unidecode(produto_base.lower())

        for product_container in response.css(config['container']):
            nome_produto_site = product_container.css(config['name']).get('')
            if nome_produto_site:
                nome_produto_site_normalizado = unidecode.unidecode(nome_produto_site.strip().lower())
                # Use comparação mais flexível
                if produto_base_normalizado in nome_produto_site_normalizado:
                    preco_lista = product_container.css(config['price']).getall()
                    preco_texto = ' '.join(preco_lista).strip()
                    preco_limpo = ''.join(filter(lambda char: char.isdigit() or char in ',.', preco_texto))
                    preco_limpo = preco_limpo.replace('.', '').replace(',', '.')
                    # Tenta extrair o link do produto (opcional)
                    link = product_container.xpath('.//ancestor::a/@href').get()
                    if link and not link.startswith('http'):
                        link = response.urljoin(link)
                    yield {
                        'produto_base': produto_base,
                        'site': site,
                        'url': link or response.url,
                        'nome_encontrado': nome_produto_site.strip(),
                        'preco': preco_limpo if preco_limpo else '0.00'
                    }
                    return
        yield {
            'produto_base': produto_base,
            'site': site,
            'url': response.url,
            'nome_encontrado': 'Nao encontrado',
            'preco': '0.00'
        }
    
    def limpar_termo_busca(self, produto):
        # Remove ou substitui caracteres problemáticos
        return (
            produto.replace('/', ' ')
                   .replace('\\', ' ')
                   .replace('"', '')
                   .replace("'", '')
                   .replace('–', '-')  # substitui traço longo por traço normal
                   .replace(',', '')
                   .replace('.', '')
                   .replace('%', '')
                   .replace('º', '')
                   .replace('ª', '')
                   .replace('°', '')
                   .strip()
        )