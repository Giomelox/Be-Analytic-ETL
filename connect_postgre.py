"""
IMPORTADOR DE DADOS PARA POSTGRESQL
========================================================================
Esta classe fornece funcionalidades para importar dados de arquivos CSV
para um banco de dados PostgreSQL, incluindo criação automática de 
banco de dados, tabelas e mapeamento de tipos de dados.

Funcionalidades:
1. Conexão com PostgreSQL
2. Criação automática de banco de dados
3. Mapeamento de tipos pandas -> PostgreSQL
4. Criação de tabelas baseadas em schema específico
5. Importação de dados CSV com pré-processamento
6. Operação automática completa
"""

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
from psycopg2 import sql
import pandas as pd
import psycopg2
import os


class PostgreSQLImporter:
    """
    Classe principal para importação de dados CSV para PostgreSQL.
    
    Esta classe gerencia toda a operação de importação, desde a conexão
    com o banco até a inserção dos dados com tipos mapeados corretamente.
    """
    
    def __init__(self, host = 'localhost', user = 'postgres', password = None, port = 5432):
        """
        Inicializa o importador com parâmetros de conexão ao PostgreSQL.
        """

        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.connection = None  # Objeto de conexão será criado posteriormente
        
    def create_connection(self, database = 'postgres'):
        """
        Estabelece conexão com o banco de dados PostgreSQL.
        """
        try:
            # Cria conexão usando psycopg2 com parâmetros fornecidos
            self.connection = psycopg2.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                port = self.port,
                database = database
            )
            print(f"Conexão com '{database}' estabelecida com sucesso.")
            return True
        except psycopg2.Error as e:
            # Captura específica de erros do PostgreSQL
            print(f"Erro ao conectar ao PostgreSQL: {e}")
            return False
        except Exception as e:
            # Captura outros erros genéricos
            print(f"Erro inesperado ao conectar: {e}")
            return False
    
    def create_database(self, db_name):
        """
        Cria um novo banco de dados se ele não existir.
        
        Para criar um banco de dados, é necessário conectar ao banco,
        pois não é possível criar banco dentro de si mesmo.
        """

        try:
            # Fecha conexão atual se estiver conectado a outro banco
            if (self.connection and 
                self.connection.get_dsn_parameters()['dbname'] != 'postgres'):
                self.connection.close()
            
            # Conecta ao banco 'postgres' (banco padrão do sistema)
            self.create_connection('postgres')
            
            # Define nível de isolamento para AUTOCOMMIT (necessário para CREATE DATABASE)
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            # Cria cursor para executar comandos SQL
            cursor = self.connection.cursor()
            
            # Verifica se o banco já existe consultando o catálogo do PostgreSQL
            cursor.execute(
                sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), 
                [db_name]
            )
            exists = cursor.fetchone()
            
            if not exists:
                # Banco não existe, então cria
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(db_name)
                    )
                )
                print(f"Banco de dados '{db_name}' criado com sucesso.")
            else:
                print(f"Banco de dados '{db_name}' já existe.")
            
            # Fecha cursor e conexão
            cursor.close()
            self.connection.close()
            return True
            
        except psycopg2.Error as e:
            print(f"Erro do PostgreSQL ao criar banco de dados: {e}")
            return False
        except Exception as e:
            print(f"Erro inesperado ao criar banco de dados: {e}")
            return False
    
    @staticmethod
    def pandas_to_sql_type(dtype):
        """
        Mapeia tipos de dados do pandas para tipos SQL genéricos.
        
        Este método estático fornece mapeamento padrão para quando não há
        um mapeamento específico definido para a coluna.
        """
        # Verifica tipo inteiro (inclui int8, int16, int32, int64)
        if pd.api.types.is_integer_dtype(dtype):
            return 'INTEGER'
        
        # Verifica tipo float (inclui float16, float32, float64)
        elif pd.api.types.is_float_dtype(dtype):
            return 'REAL'  # Ponto flutuante de precisão simples
        
        # Verifica tipo booleano
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        
        # Verifica tipo datetime (qualquer variação)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        
        # Para todos os outros casos (strings, objetos, etc.)
        else:
            return 'TEXT'
    
    def get_column_type_for_your_table(self, column_name, dtype):
        """
        Mapeia tipos específicos para colunas baseado em schema personalizado.
        
        Este método implementa o mapeamento específico para as colunas
        do dataset IDA da Anatel. Se a coluna não estiver no mapeamento
        específico, usa o mapeamento genérico.
        """

        # Normaliza nome da coluna: minúsculas, sem espaços, underline como separador
        clean_col = column_name.strip().lower().replace(' ', '_')
        
        # Mapeamento específico para as colunas do dataset IDA
        type_mapping = {
            'id': 'INTEGER PRIMARY KEY',        # Chave primária
            'grupo_economico': 'TEXT',          # Texto livre
            'servico': 'TEXT',                  # Texto livre
            'mes_referencia': 'DATE',           # Data (apenas data, sem hora)
            'valor': 'DOUBLE PRECISION',        # Número decimal de alta precisão
            'tipo_servico': 'TEXT'              # Texto livre (SCM, SMP, STFC)
        }
        
        # Se a coluna está no mapeamento específico, usa o tipo definido
        if clean_col in type_mapping:
            return type_mapping[clean_col]
        
        # Se não está no mapeamento específico, usa mapeamento genérico
        return self.pandas_to_sql_type(dtype)
    
    def preprocess_dataframe(self, df):
        """
        Pré-processa o DataFrame antes da importação para o banco de dados.
        
        Aplica transformações básicas para garantir compatibilidade
        entre os dados do CSV e o esquema do banco de dados.
        """

        # Converte coluna de referência de mês para tipo datetime
        if 'referencia_mes' in df.columns:
            df['referencia_mes'] = pd.to_datetime(
                df['referencia_mes'], 
                errors='coerce' # converte valores inválidos para NaT
            )
        
        # Processa coluna de valor (já tratada no código anterior)
        if 'valor' in df.columns:
            # Converte strings vazias para None (NULL no SQL)
            df['valor'] = df['valor'].replace('', None)
        
        # Normaliza nomes das colunas para padrão SQL
        df.columns = [
            col.replace(' ', '_')   # Espaços para underline
               .replace('-', '_')   # Hífens para underline
               .replace('.', '_')   # Pontos para underline
               .lower()             # Tudo minúsculo
            for col in df.columns
        ]
        
        return df
    
    def create_table_from_csv(self, csv_path, table_name, db_name, 
                          delimiter=',', encoding = 'utf-8'):
        """
        Cria tabela e importa dados de um arquivo CSV.
        
        Esta é a função principal que lê o CSV, cria a tabela com schema
        apropriado e insere todos os dados no banco.
        """
        try:
            # Conecta ao banco de dados especificado
            if not self.create_connection(db_name):
                return False
            
            cursor = self.connection.cursor()
            
            # ETAPA 1: LER ARQUIVO CSV
            print(f"Lendo arquivo CSV: {csv_path}")
            df = pd.read_csv(csv_path, delimiter=delimiter, encoding=encoding)
            
            # ETAPA 2: PRÉ-PROCESSAR DADOS
            df = self.preprocess_dataframe(df)
            
            # ETAPA 3: PREPARAR DEFINIÇÃO DE COLUNAS PARA CREATE TABLE
            columns_def = []
            for col in df.columns:
                # Determina tipo SQL apropriado para cada coluna
                sql_type = self.get_column_type_for_your_table(col, df[col].dtype)
                columns_def.append(f"{col} {sql_type}")
            
            # REMOVER TABELA EXISTENTE SE HOUVER (EVITA DUPLICATAS)
            print(f"Verificando tabela existente: {table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            print(f"Tabela '{table_name}' limpa (se existia)")
            
            # Query CREATE TABLE
            create_table_query = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns_def)}
            );
            """
            
            # Executa criação da tabela
            cursor.execute(create_table_query)
            print(f"Tabela '{table_name}' criada com sucesso.")
            print(f"Inserindo dados no banco.")
            
            # ETAPA 4: PREPARAR DADOS PARA INSERÇÃO
            data_tuples = []
            for _, row in df.iterrows():
                values = []
                for val in row:
                    if pd.isna(val):
                        # Valores nulos do pandas -> None (NULL do SQL)
                        values.append(None)
                    else:
                        # Converte para tipo Python nativo apropriado
                        if isinstance(val, pd.Timestamp):
                            # Timestamp do pandas -> datetime do Python
                            values.append(val.to_pydatetime())
                        elif pd.api.types.is_integer_dtype(type(val)):
                            # Inteiros do pandas -> int do Python
                            values.append(int(val))
                        elif pd.api.types.is_float_dtype(type(val)):
                            # Floats do pandas -> float do Python
                            values.append(float(val))
                        else:
                            # Outros tipos (strings) -> str do Python
                            values.append(str(val))
                data_tuples.append(tuple(values))
            
            # ETAPA 5: INSERIR DADOS EM LOTE
            # Cria string de placeholders para prepared statement
            placeholders = ', '.join(['%s'] * len(df.columns))
            columns_str = ', '.join(df.columns)
            
            # Query INSERT
            insert_query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders})
            """
            
            # Executa inserção em lote
            cursor.executemany(insert_query, data_tuples)
            
            # ETAPA 6: CONFIRMAR TRANSAÇÃO
            self.connection.commit()
            print(f"✓ {len(data_tuples):,} registros importados com sucesso.")
            
            # Fecha cursor e conexão
            cursor.close()
            self.connection.close()
            return True
            
        except pd.errors.EmptyDataError:
            print(f"Arquivo CSV está vazio: {csv_path}")
            return False
        except pd.errors.ParserError:
            print(f"Erro ao ler arquivo CSV (formato inválido): {csv_path}")
            return False
        except psycopg2.Error as e:
            print(f"Erro do PostgreSQL: {e}")
            import traceback
            traceback.print_exc()
            if self.connection:
                self.connection.rollback()  # Reverte em caso de erro
            return False
        except Exception as e:
            print(f"✗Erro inesperado: {e}")
            import traceback
            traceback.print_exc()
            if self.connection:
                self.connection.rollback()  # Reverte em caso de erro
            return False
    
    def import_csv_automatic(self, csv_path, db_name, table_name = None, 
                             delimiter = ',', encoding = 'utf-8'):
        """
        Método automático completo para importação de CSV.
        
        Orquestra todo o processo:
        1. Determina nome da tabela
        2. Cria banco de dados
        3. Cria tabela e importa dados
        """

        # Se nome da tabela não foi fornecido, usa nome do arquivo
        if table_name is None:
            # Extrai nome base do arquivo (sem extensão)
            base_name = os.path.splitext(os.path.basename(csv_path))[0].lower()
            # Normaliza para nome de tabela válido (snake_case)
            table_name = base_name.replace(' ', '_').replace('-', '_')
            print(f"Nome da tabela não fornecido. Usando: {table_name}")
        
        # ETAPA 1: CRIAR BANCO DE DADOS (SE NECESSÁRIO)
        print(f"\nCriando banco de dados '{db_name}'...")
        if not self.create_database(db_name):
            print(f"Falha ao criar banco de dados '{db_name}'")
            return False
        
        # ETAPA 2: IMPORTAR DADOS DO CSV
        print(f"\nImportando dados para tabela '{table_name}'.")
        success = self.create_table_from_csv(
            csv_path, 
            table_name, 
            db_name, 
            delimiter, 
            encoding
        )

        # ETAPA 3: RELATÓRIO FINAL
        if success:
            print(f"\n{'='*50}")
            print("Processo concluído com sucesso.")
            print(f"{'='*50}")
            print(f"Banco de dados: {db_name}")
            print(f"Tabela: {table_name}")
            print(f"Arquivo importado: {csv_path}")
            print(f"{'='*50}")
        else:
            print(f"\nOcorreu um erro durante a importação.")
        
        return success

# FUNÇÃO PRINCIPAL
def main():
    """
    Função principal que demonstra o uso da classe PostgreSQLImporter.
    
    Esta função:
    1. Carrega configurações de variáveis de ambiente
    2. Cria instância do importador
    3. Testa conexão
    4. Executa importação automática
    """

    # Carrega variáveis de ambiente do arquivo token.env
    load_dotenv('token.env')
    
    # Configurações de conexão (carregadas do ambiente)
    config = {
        'host': os.getenv('pg_host', 'localhost'),      # Host do PostgreSQL
        'user': os.getenv('pg_user', 'postgres'),       # Usuário do PostgreSQL
        'password': os.getenv('pg_password'),           # Senha
        'port': int(os.getenv('pg_port', 5432))         # Porta (converte para int)
    }
    
    # Verifica se a senha foi fornecida
    if not config['password']:
        print("Senha do PostgreSQL não configurada.")
        return
    
    # Caminho do arquivo CSV a ser importado
    csv_file = 'dados_ida_tratados.csv'
    
    # Verifica se o arquivo CSV existe
    if not os.path.exists(csv_file):
        print(f"Arquivo CSV não encontrado: {csv_file}")
        print("Certifique-se de que o arquivo existe no diretório atual")
        return
    
    # Nomes para banco de dados e tabela
    database_name = 'be_analytic_database'  # Nome do banco de dados
    table_name = 'be_analytic_table'        # Nome da tabela
    
    print("Iniciando importador para o postgreL")
    print("=" * 50)
    
    # Cria instância do importador com configurações
    importer = PostgreSQLImporter(**config)
    
    # Testa conexão básica com PostgreSQL
    print("\nTestando conexão com PostgreSQL.")
    if importer.create_connection('postgres'):
        print("Conexão com PostgreSQL bem sucedida.")
    else:
        print("Não foi possível conectar ao PostgreSQL.")
        print("Verifique as configurações em token.env")
        return
    
    # Executa importação automática completa
    print(f"\nIniciando importação.")
    success = importer.import_csv_automatic(
        csv_path = csv_file,
        db_name = database_name,
        table_name = table_name,
        delimiter = ',',    # Delimitador do CSV
        encoding = 'utf-8'  # Codificação
    )
    
    # Resultado final
    if success:
        print("\nImportação finalizada com sucesso!")
    else:
        print("\nHouve problemas na importação. Verifique os erros acima.")


# PONTO DE ENTRADA DO SCRIPT
if __name__ == "__main__":
    """
    Ponto de entrada principal do script.
    
    Executa a função main() quando o script é rodado diretamente.
    """
    main()