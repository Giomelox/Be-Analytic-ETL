"""
COLETOR DE DADOS DO ÍNDICE DE DESEMPENHO NO ATENDIMENTO (IDA) - ANATEL
========================================================================
Este script coleta, processa e consolida dados dos serviços de telecomunicações
(SCM, SMP, STFC) do portal de dados abertos da Anatel.

Funcionalidades:
1. Busca dinâmica do ID do dataset na API
2. Download de arquivos ODS e CSV
3. Processamento e limpeza dos dados
4. Consolidação em um único arquivo CSV
"""

from io import StringIO 
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd 
import requests 
import os 
import re 

# Carrega variáveis de ambiente do arquivo token.env
load_dotenv('token.env')
API_KEY = os.getenv('api_key')

# Cabeçalhos HTTP usados nas requisições API
HEADERS = {
    'accept': 'application/json',
    'chave-api-dados-abertos': API_KEY
}

# URL base da API de dados abertos
BASE_URL = 'https://dados.gov.br/dados/api/publico'

def buscar_dataset_id_dinamicamente():
    """
    Busca o ID do dataset 'Índice de Desempenho no Atendimento' dinamicamente.
    
    Esta função consulta a API usando o endpoint de busca e retorna o ID do dataset.
    """
    print('Buscando ID do dataset "Índice de Desempenho no Atendimento".')
    
    # URL para busca de datasets
    url = f'{BASE_URL}/conjuntos-dados'
    
    # Parâmetros de busca
    params = {
        'nomeConjuntoDados': 'indice-desempenho-atendimento',
        'dadosAbertos': 'true',
        'isPrivado': 'false',
        'pagina': 1
    }
    
    try:
        # Faz requisição GET à API com timeout de 15 segundos
        response = requests.get(url, headers = HEADERS, params = params, timeout = 15)
        response.raise_for_status()  # Lança exceção se status não for 2xx
        
       # Converte resposta JSON para dicionário Python
        datasets = response.json()
        
        # Verifica se a lista não está vazia
        if not datasets:
            print('Nenhum dataset encontrado')
            return None
        
        # Pega o primeiro dataset da lista (normalmente é o único)
        dataset = datasets[0]
        dataset_id = dataset.get('id')
        dataset_title = dataset.get('title', 'Sem título')
        
        if dataset_id:
            print(f'Dataset encontrado: {dataset_title}')
            print(f'ID: {dataset_id}')
            return dataset_id
        else:
            print('Dataset encontrado, mas sem campo "id"')
            return None
            
    except requests.exceptions.RequestException as e:
        print(f'Erro na requisição: {e}')
        return None
    except Exception as e:
        print(f'Erro inesperado: {e}')
        return None


def fazer_requisicao_api(dataset_id):
    """
    Faz requisição à API para obter os arquivos de um dataset específico.
    """
    print(f'Conectando à API para dataset ID: {dataset_id}')

    # Monta URL específica para o dataset
    url = f'{BASE_URL}/conjuntos-dados/{dataset_id}'
    
    try:
        # Faz requisição GET ao endpoint específico do dataset
        response = requests.get(url, headers = HEADERS, timeout = 15)
        response.raise_for_status()  # Lança exceção se status não for 2xx
        
        # Converte resposta JSON para dicionário Python
        dados = response.json()
        
        # Extrai lista de arquivos disponíveis
        recursos = dados.get('recursos', [])
        print(f'Conexão bem-sucedida: {len(recursos)} recursos disponíveis')
        
        return dados
        
    except requests.exceptions.Timeout:
        raise Exception('Timeout na requisição à API')
    except requests.exceptions.HTTPError as e:
        raise Exception(f'Erro HTTP {e.response.status_code}: {e.response.text}')
    except Exception as e:
        raise Exception(f'Falha na requisição: {e}')

# ============================================================================
# FUNÇÕES DE PROCESSAMENTO DE DADOS
# ============================================================================

def extrair_urls_relevantes(dados):
    """
    Filtra os recursos para extrair apenas URLs de arquivos ODS/CSV relevantes.
    
    Filtra por:
    1. Serviços: SCM, SMP, STFC
    2. Formatos: Apenas arquivos ODS ou CSV
    """

    # Obtém lista de recursos da resposta da API
    recursos = dados.get('recursos', [])
    urls_filtradas = []  # Lista para armazenar recursos filtrados
    
    print(f'\nFiltrando recursos por SCM/SMP/STFC')
    
    # Itera sobre cada recurso disponível
    for recurso in recursos:
        # Extrai informações básicas do recurso
        link = recurso.get('link', '')
        titulo = recurso.get('titulo', '')
        formato = recurso.get('formato', '')
        
        # Ignora recursos sem link
        if not link:
            continue
        
        # Corrige possíveis barras invertidas na URL
        link_corrigido = link.replace('\\', '/')
        
        # Converte título para maiúsculas para comparação case-insensitive
        titulo_upper = titulo.upper()
        
        # Filtra apenas serviços SCM, SMP e STFC
        if any(servico in titulo_upper for servico in ['SCM', 'SMP', 'STFC']):
            # Aceita apenas arquivos ODS ou CSV
            if 'ODS' in formato.upper() or '.ODS' in link_corrigido.upper() or '.CSV' in link_corrigido.upper():
                # Identifica tipo de serviço e extrai ano do título
                servico = identificar_servico(titulo)
                
                # Armazena informações relevantes do arquivo
                urls_filtradas.append({
                    'url': link_corrigido,
                    'titulo': titulo,
                    'formato': formato,
                    'ano': extrair_ano_titulo(titulo),
                    'servico': servico
                })
    
    print(f'Encontrados {len(urls_filtradas)} arquivos relevantes')
    return urls_filtradas


def identificar_servico(titulo):
    """
    Identifica o tipo de serviço baseado no título do arquivo.
    """
    # Normaliza título para maiúsculas para comparação
    titulo_upper = titulo.upper()
    
    # Verifica presença de cada sigla de serviço
    if 'SCM' in titulo_upper:
        return 'SCM'  # Serviço de Comunicação Multimídia
    elif 'SMP' in titulo_upper:
        return 'SMP'  # Serviço Móvel Pessoal
    elif 'STFC' in titulo_upper:
        return 'STFC'  # Serviço de Telefonia Fixa Comutada
    else:
        return 'OUTROS'


def extrair_ano_titulo(titulo):
    """
    Extrai o ano do título do arquivo usando expressão regular.
    """
    # Procura por sequência de 4 dígitos no título
    match = re.search(r'(\d{4})', titulo)
    # Retorna o ano como inteiro se encontrado, None caso contrário
    return int(match.group(1)) if match else None


def encontrar_linha_inicio_dados(df):
    """
    Encontra a linha onde começam os dados reais no DataFrame.
    
    Procura pela linha que contém 'GRUPO ECONÔMICO' ou 'GRUPO_ECON'
    que geralmente é o cabeçalho dos dados.
    """

    # Percorre todas as linhas do DataFrame
    for idx, linha in df.iterrows():
        # Concatena todos os valores da linha em uma string
        linha_str = ' '.join(str(val) for val in linha.values if pd.notna(val))
        # Verifica se a linha contém o marcador de cabeçalho
        if 'GRUPO ECONÔMICO' in linha_str.upper() or 'GRUPO_ECON' in linha_str.upper():
            return idx
    # Se não encontrar, assume que dados começam na primeira linha
    return 0


def extrair_dados_reais(df, url_info):
    """
    Remove cabeçalhos e metadados, extraindo apenas os dados reais.
    """

    # Identifica a linha onde começam os dados válidos
    inicio_dados = encontrar_linha_inicio_dados(df)
    
    if inicio_dados > 0:
        # A linha de início contém os cabeçalhos corretos
        novos_cabecalhos = df.iloc[inicio_dados].tolist()
        
        # Pega os dados a partir da linha seguinte
        dados_reais = df.iloc[inicio_dados + 1:].reset_index(drop = True)
        
        # Define os novos cabeçalhos
        dados_reais.columns = novos_cabecalhos
        
        # Lista de padrões de texto que indicam metadados
        padroes_metadados = [
            'SERVIÇO:', 'PERÍODO:', 'FONTE:', 'Para maiores informações',
            'ÍNDICE DE DESEMPENHO NO ATENDIMENTO', 'ANATEL'
        ]
        
        # Remove linhas que contenham textos de metadados
        for col in dados_reais.columns:
            if col and isinstance(col, str):
                for padrao in padroes_metadados:
                    dados_reais = dados_reais[
                        ~dados_reais[col].astype(str).str.contains(padrao, case = False, na = False)
                    ]
        
        # Remove linhas totalmente vazias
        return dados_reais.dropna(how = 'all')
    
    # Caso não encontre cabeçalho específico, retorna DataFrame original
    return df

def processar_arquivo_ods(conteudo, url_info):
    """
    Processa arquivo no formato ODS (OpenDocument Spreadsheet).
    """

    # Cria nome de arquivo temporário baseado no hash do título
    temp_file = f'temp_{hash(url_info["titulo"])}.ods'
    
    try:
        # Salva conteúdo binário em um arquivo temporário
        with open(temp_file, 'wb') as f:
            f.write(conteudo)
        
        # Lê arquivo ODS sem cabeçalho (engine 'odf' para formato OpenDocument)
        df = pd.read_excel(temp_file, engine = 'odf', header = None, dtype = str)
        
        # Remove arquivo temporário após leitura
        os.remove(temp_file)
        
        # Extrai apenas os dados reais
        df = extrair_dados_reais(df, url_info)
        
        # Verifica se há dados após limpeza
        if df.empty:
            print('Nenhum dado válido encontrado')
            return None
        
        # Renomeia as duas primeiras colunas para nomes padronizados
        if len(df.columns) >= 2:
            df = df.rename(columns={
                df.columns[0]: 'GRUPO_ECONOMICO',
                df.columns[1]: 'VARIAVEL'
            })

        # Aplica limpezas e transformações
        df = limpar_valores_decimais(df)
        df = normalizar_colunas_data(df)
        
        # Converte para formato longo
        return transformar_para_formato_longo(df, url_info)
        
    except Exception as e:
        print(f'Erro ao processar ODS: {e}')
        # Remove arquivo temporário se ainda existir
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return None


def processar_arquivo_csv(conteudo, url_info):
    """
    Processa arquivo no formato CSV.
    """

    try:
        # Tenta diferentes codificações
        for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
            try:
                # Decodifica conteúdo binário para string
                conteudo_str = conteudo.decode(encoding)
                # Lê CSV com separador de tabulação
                df = pd.read_csv(StringIO(conteudo_str), header = None, sep = '\t', dtype = str)
                break  # Sai do loop se conseguir ler
            except:
                continue  # Tenta a próxima codificação
        
        # Extrai dados reais
        df = extrair_dados_reais(df, url_info)
        
        # Renomeia colunas de identificação
        if len(df.columns) >= 2:
            df = df.rename(columns={
                df.columns[0]: 'GRUPO_ECONOMICO',
                df.columns[1]: 'VARIAVEL'
            })
        
        # Aplica limpezas e transformações
        df = limpar_valores_decimais(df)
        df = normalizar_colunas_data(df)
        
        # Converte para formato longo
        return transformar_para_formato_longo(df, url_info)
        
    except Exception as e:
        print(f'Erro ao processar CSV: {e}')
        return None


def baixar_arquivo(url_info):
    """
    Baixa o conteúdo de um arquivo a partir da URL.
    """
    print(f'Baixando: {url_info["titulo"]}')
    
    try:
        # Primeira tentativa com headers de autenticação
        response = requests.get(url_info['url'], headers = HEADERS, timeout = 20)
        
        # Se falhar com autenticação, tenta sem (alguns arquivos são públicos)
        if response.status_code != 200:
            response = requests.get(url_info['url'], timeout = 20)
        
        # Verifica se requisição foi bem-sucedida
        response.raise_for_status()
        
        # Retorna conteúdo binário
        return response.content
        
    except Exception as e:
        print(f'Erro ao baixar: {e}')
        return None


def processar_arquivo_individual(url_info):
    """
    Processa um arquivo individual (rota para ODS ou CSV).
    """

    print(f'\nProcessando: {url_info["titulo"]}')
    
    # Baixa conteúdo do arquivo
    conteudo = baixar_arquivo(url_info)
    if conteudo is None:
        return None
    
    # Normaliza URL e formato para comparação case-insensitive
    url = url_info['url'].upper()
    formato = url_info.get('formato', '').upper()
    
    # Escolhe processador baseado no formato do arquivo
    if '.ODS' in url or 'ODS' in formato:
        return processar_arquivo_ods(conteudo, url_info)
    elif '.CSV' in url or 'CSV' in formato:
        return processar_arquivo_csv(conteudo, url_info)
    
    # Formato não suportado
    print('Formato não suportado')
    return None

def normalizar_colunas_data(df):
    """
    Normaliza colunas de data para formato padrão 'YYYY-MM'.
    
    Converte formatos como '2013-01-01 00:00:00' para '2013-01'.
    """

    # Cria cópia para evitar modificar o original
    df_normalizado = df.copy()
    
    # Percorre todas as colunas
    for coluna in df.columns:
        coluna_str = str(coluna)
        
        # Ignora colunas já no formato YYYY-MM
        if re.match(r'\d{4}-\d{2}', coluna_str):
            continue
        
        # Trata colunas com timestamp completo
        elif re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', coluna_str):
            try:
                # Converte string para objeto datetime
                data_obj = datetime.strptime(coluna_str, '%Y-%m-%d %H:%M:%S')
                # Formata para YYYY-MM
                nome_normalizado = data_obj.strftime('%Y-%m')
                # Renomeia a coluna
                df_normalizado = df_normalizado.rename(columns={coluna: nome_normalizado})
            except:
                continue  # Se falhar, mantém coluna original
    
    return df_normalizado


def limpar_valores_decimais(df):
    """
    Remove zeros decimais desnecessários de valores numéricos.
    
    Exemplo: '15.00' -> '15', '15.50' -> '15.5'
    """
    # Cria cópia para evitar modificar o original
    df_limpo = df.copy()
    
    # Percorre todas as colunas
    for coluna in df_limpo.columns:
        # Ignora colunas de identificação (não são valores numéricos)
        if coluna in ['GRUPO_ECONOMICO', 'VARIAVEL', 'OPERADORA']:
            continue
        # Ignora colunas que representam datas
        if re.match(r'\d{4}-\d{2}', str(coluna)):
            continue
        
        # Função interna para limpar cada valor individualmente
        def limpar_celula(valor):
            if pd.isna(valor):
                return valor  # Mantém valores nulos
            
            valor_str = str(valor).strip()
            
            # Verifica se é número com parte decimal
            if '.' in valor_str and valor_str.replace('.', '').isdigit():
                partes = valor_str.split('.')
                if len(partes) == 2:
                    parte_inteira = partes[0]
                    parte_decimal = partes[1]
                    
                    # Remove decimais que são apenas zeros
                    if parte_decimal.replace('0', '') == '':
                        return parte_inteira  # Ex: '15.00' -> '15'
                    else:
                        # Remove zeros à direita da parte decimal
                        parte_decimal_limpa = parte_decimal.rstrip('0')
                        if parte_decimal_limpa:
                            return f'{parte_inteira}.{parte_decimal_limpa}'  # Ex: '15.50' -> '15.5'
                        else:
                            return parte_inteira  # Ex: '15.0' -> '15'
            
            return valor_str  # Retorna valor original se não for número decimal
        
        # Aplica limpeza a toda a coluna
        df_limpo[coluna] = df_limpo[coluna].apply(limpar_celula)
    
    return df_limpo


def transformar_para_formato_longo(df, url_info):
    """
    Transforma DataFrame de formato largo para formato longo.
    
    Formato largo: Colunas representam meses, linhas representam variáveis
    Formato longo: Cada linha tem uma observação (variável + mês + valor)
    """

    # Listas para classificar colunas
    colunas_id = []  # Colunas de identificação
    colunas_data = []  # Colunas que representam datas/meses
    
    # Classifica cada coluna
    for coluna in df.columns:
        coluna_str = str(coluna)
        
        # Colunas de identificação
        if coluna_str in ['GRUPO_ECONOMICO', 'VARIAVEL', 'OPERADORA']:
            colunas_id.append(coluna)
        # Colunas de data (formato YYYY-MM)
        elif re.match(r'\d{4}-\d{2}', coluna_str):
            colunas_data.append(coluna)
    
    # Se não houver colunas suficientes, mantém formato original
    if not colunas_id or not colunas_data:
        df['SERVICO'] = url_info['servico']
        return df
    
    try:
        # Usa melt para transformar de largo para longo
        df_longo = pd.melt(
            df,
            id_vars = colunas_id,  # Colunas que identificam cada linha
            value_vars = colunas_data,  # Colunas a serem transformadas
            var_name = 'REFERENCIA_MES',  # Nome da nova coluna com os meses
            value_name = 'VALOR'  # Nome da nova coluna com os valores
        )
        
        # Converte referência de mês para datetime
        df_longo['REFERENCIA_MES'] = pd.to_datetime(
            df_longo['REFERENCIA_MES'],
            format='%Y-%m',
            errors='coerce'  # Converte erros para NaT
        )
        
        # Adiciona coluna com tipo de serviço
        df_longo['SERVICO'] = url_info['servico']
        
        # Função para converter valores para formato numérico padronizado
        def converter_para_numero(valor):
            if pd.isna(valor):
                return ''  # Retorna string vazia para nulos
            
            valor_str = str(valor).strip()
            
            # Valores inválidos comuns
            valores_invalidos = ['', 'nan', 'NaN', '-', '--', '---', 'ND', 'N/D']
            if valor_str in valores_invalidos:
                return ''
            
            # Remove caracteres não numéricos
            valor_limpo = re.sub(r'[^\d\.,]', '', valor_str)
            
            if not valor_limpo:
                return ''
            
            try:
                # Trata formato brasileiro (1.234,56 -> 1234.56)
                if ',' in valor_limpo:
                    if '.' in valor_limpo:
                        # Formato com separador de milhar e decimal: 1.234,56
                        partes_virgula = valor_limpo.split(',')
                        parte_inteira = partes_virgula[0].replace('.', '')  # Remove pontos
                        if len(partes_virgula) > 1:
                            parte_decimal = partes_virgula[1]
                            return f'{parte_inteira}.{parte_decimal}'
                        else:
                            return parte_inteira
                    else:
                        # Formato com vírgula decimal: 1234,56 -> 1234.56
                        return valor_limpo.replace(',', '.')
                
                # Trata números com ponto decimal
                elif '.' in valor_limpo:
                    partes = valor_limpo.split('.')
                    if len(partes) > 2:
                        # Múltiplos pontos = separador de milhar: 1.234.56 -> 123456
                        return ''.join(partes)
                    else:
                        # Ponto decimal normal
                        return valor_limpo
                else:
                    # Número inteiro
                    return valor_limpo
                    
            except Exception:
                # Se houver erro, retorna valor original
                return valor_str
        
        # Aplica conversão a todos os valores
        df_longo['VALOR'] = df_longo['VALOR'].apply(converter_para_numero)
        
        return df_longo
        
    except Exception as e:
        # Em caso de erro, mantém formato original
        print(f'Erro na transformação: {e}')
        df['SERVICO'] = url_info['servico']
        return df

def criar_dataframe_consolidado():
    """
    Função principal que orquestra todo o processo de coleta e consolidação.
    
    Fluxo:
    1. Busca ID do dataset dinamicamente
    2. Obtém recursos da API
    3. Filtra URLs relevantes
    4. Processa cada arquivo individualmente
    5. Consolida todos os DataFrames
    6. Aplica limpezas finais
    """

    # Cabeçalho visual
    print('=' * 80)
    print('                     COLETOR DE DADOS IDA - ANATEL')
    print('=' * 80)
    
    try:
        # ETAPA 1: BUSCAR ID DO DATASET DINAMICAMENTE
        dataset_id = buscar_dataset_id_dinamicamente()
        
        if dataset_id is None:
            raise Exception('Não foi possível encontrar o dataset ID.')
        
        # ETAPA 2: OBTER DADOS DA API
        dados_api = fazer_requisicao_api(dataset_id)
        
        # ETAPA 3: FILTRAR URLs RELEVANTES
        urls = extrair_urls_relevantes(dados_api)
        
        if not urls:
            raise Exception('Nenhum arquivo relevante encontrado')

        # ETAPA 4: PROCESSAR CADA ARQUIVO
        dataframes = []
        print(f'\nProcessando {len(urls)} arquivos...')
        
        for i, url_info in enumerate(urls, 1):
            print(f'\n[{i}/{len(urls)}]', end='')
            df = processar_arquivo_individual(url_info)
            
            # Adiciona apenas DataFrames válidos e não vazios
            if df is not None and len(df) > 0:
                dataframes.append(df)
                print('Baixado com sucesso')
        
        # Verifica se algum arquivo foi processado com sucesso
        if not dataframes:
            raise Exception('Nenhum dado pôde ser processado')
        
        # ETAPA 5: CONSOLIDAR TODOS OS DATAFRAMES
        df_consolidado = pd.concat(dataframes, ignore_index = True)
        
        # Remove duplicatas exatas
        df_consolidado = df_consolidado.drop_duplicates()
        
        # ETAPA 6: LIMPEZA FINAL DE VALORES
        if 'VALOR' in df_consolidado.columns:
            def limpar_valor_final(valor):
                if pd.isna(valor) or valor == '':
                    return ''
                
                valor_str = str(valor)
                
                # Remove zeros decimais desnecessários
                if '.' in valor_str:
                    partes = valor_str.split('.')
                    parte_inteira = partes[0]
                    parte_decimal = partes[1].rstrip('0')  # Remove zeros à direita
                    
                    if parte_decimal:
                        return f'{parte_inteira}.{parte_decimal}'
                    else:
                        return parte_inteira
                
                return valor_str
            
            df_consolidado['VALOR'] = df_consolidado['VALOR'].apply(limpar_valor_final)
        
        # ETAPA 7: RENOMEAR COLUNAS PARA PADRÃO FINAL
        renomear_colunas = {}
        
        if 'GRUPO_ECONOMICO' in df_consolidado.columns:
            renomear_colunas['GRUPO_ECONOMICO'] = 'grupo_economico'
        
        if 'VARIAVEL' in df_consolidado.columns:
            renomear_colunas['VARIAVEL'] = 'servico'
        
        if 'REFERENCIA_MES' in df_consolidado.columns:
            renomear_colunas['REFERENCIA_MES'] = 'mes_referencia'
        
        if 'VALOR' in df_consolidado.columns:
            renomear_colunas['VALOR'] = 'valor'
        
        if 'SERVICO' in df_consolidado.columns:
            renomear_colunas['SERVICO'] = 'tipo_servico'
        
        # Aplica renomeação se houver colunas para renomear
        if renomear_colunas:
            df_consolidado = df_consolidado.rename(columns = renomear_colunas)
        
        # ETAPA 8: ADICIONAR ID SEQUENCIAL E ORGANIZAR COLUNAS
        # Insere coluna ID sequencial começando em 1
        df_consolidado.insert(0, 'id', range(1, len(df_consolidado) + 1))
        
        # Define ordem preferencial das colunas
        colunas_desejadas = ['id', 'grupo_economico', 'servico', 'mes_referencia', 'valor', 'tipo_servico']
        colunas_existentes = [c for c in colunas_desejadas if c in df_consolidado.columns]
        
        # Preserva colunas adicionais que possam existir
        colunas_extras = [c for c in df_consolidado.columns if c not in colunas_desejadas]
        df_consolidado = df_consolidado[colunas_existentes + colunas_extras]
        
        # ETAPA 9: RELATÓRIO FINAL
        print(f'\nCOLETA CONCLUÍDA COM SUCESSO!')
        print(f'Total de registros: {len(df_consolidado):,}')
        
        return df_consolidado
        
    except Exception as e:
        # Log de erro detalhado
        print(f'\nErro na Coleta: {e}')
        raise

# FUNÇÃO MAIN

def main():
    """
    Função principal que executa todo o fluxo do coletor.
    
    Fluxo:
    1. Executa coleta e consolidação
    2. Salva dados em arquivo CSV
    3. Exibe estatísticas finais
    """
    try:
        print('INICIANDO COLETOR DE DADOS IDA - ANATEL')
        print('=' * 40)
        
        # Executa processo completo de coleta
        df_final = criar_dataframe_consolidado()
        
        if df_final is not None:
            # SALVAR DADOS EM ARQUIVO
            # Cria nome de arquivo com timestamp para evitar sobrescrita
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            nome_arquivo = f'dados_ida_tratados.csv'
            
            # Salva DataFrame em CSV com codificação UTF-8
            df_final.to_csv(nome_arquivo, index=False, encoding='utf-8')
            
            print(f'\nDados salvos em: {nome_arquivo}')
            
            print(f'\nProcesso concluído com sucesso.')
            return df_final
        
    except Exception as e:
        print(f'\nProcesso Interrompido: {e}')
        return None

# PONTO DE ENTRADA DO SCRIPT
if __name__ == '__main__':
    """
    Ponto de entrada principal do script.
    
    Executa a função main() quando o script é rodado diretamente.
    """
    resultado = main()