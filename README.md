# Be-Analytic-ETL

## Antes de começarmos, vale ressaltar que o arquivo de token.env está no seu formato original à fins de praticidade e correto funcionamento (mesmo não sendo aconselhável para produção). Sendo assim, é aconselhável não alterar este arquivo caso você não tenha um token de acesso para a API, ou não quiser configurar informações da conexão postgre.

# Configurando token e testando endpoint (Pule esta etapa caso queira utilizar o código original)

Caso você queira configurar um token, acesse este site: https://dados.gov.br/auth/minha-conta e entre com sua conta GOV e clique em 'Gerar', após isso, clique em 'Ver' para copiar e colar o token no arquivo .env

<img width="1465" height="432" alt="image" src="https://github.com/user-attachments/assets/30db25ed-dd8e-4328-906a-3b8f19938f11" />

Como testar o endpoint: Caso você queira se certificar que o endpoint da API está correto, acesse este site: https://dados.gov.br/swagger-ui/index.html#/Conjuntos%20de%20dados/listar
Você irá se deparar com um botão chamado 'Authorize', clique nele, cole seu token gerado no passo anterior e clique novamente em 'Authorize' para validar.

<img width="1839" height="430" alt="image" src="https://github.com/user-attachments/assets/19763378-839b-417b-b369-6adb27650034" />

Feito isso, clique na caixa chamada '/dados/api/publico/conjuntos-dados' e clique em 'Try Out' (ou 'Tentar') no canto superior direito da caixa de diálogo expandida.

Em seguida, cole as seguintes informações da imagem, e clique em executar.

<img width="1845" height="730" alt="image" src="https://github.com/user-attachments/assets/b5df04b6-e1d3-4d5c-8abf-82435aee43da" />

Você verá logo abaixo a resposta do endpoint com um json contendo os dados do dataset, esse é o json utilizado no código para extrair as tabelas.


# Rodando o container

Caso você não tenha copiado este projeto para o seu computador, será necessário fazer agora. Salve este projeto em alguma pasta.
Os próximos passos levam em consideração que você tem o projeto salvo.

### Para começar, você precisa startar seu serviço docker.

## Passo a passo para windows
Abra o Docker Desktop e aguarde até que o serviço seja startado.

Com o serviço startado, abra o VsCode, abra o arquivo onde você salvou este projeto e crie um novo terminal BASH.

Digite o comando abaixo para conteinerizar o projeto:

`````
docker compose up --build
`````


## Passo a passo para Linux
Rode o código abaixo no terminal shell para verificar se o docker já está ativo:

`````
sudo systemctl status docker
`````

Caso não esteja, rode este código abaixo para startar o serviço:

`````
sudo systemctl start docker
`````

Com o docker startado, agora nós vamos navegar até a nossa pasta do projeto:

`````
cd /caminho/onde_voce/salvou/meu_projeto
`````

Com isso, estaremos dentro da nossa pasta principal do projeto, então basta conteinerizar o projeto rodando o código:

`````
docker compose up --build
`````

# Visualizando o banco de dados

À princípio, nós podemos visualizar tudo com comandos bash, porém para ficar visualmente mais fácil, vamos abrir o navegador (Chrome, Edge ou qualquer navegador da sua escolha) e digitar na barra de pesquisa:

"http://localhost:5050/browser/"

Serão solicitados um login de usuário e uma senha, porém já configuramos isso no nosso arquivo .yml, então basta entrar:

Usuário: admin@admin.com
Senha: admin

Entrando com sucesso, vamos ter a tela do pgadmin, então basta criarmos um servidor:

<img width="1860" height="956" alt="image" src="https://github.com/user-attachments/assets/22cce5b2-590e-445f-9359-9fc882ecf9c8" />

Digite o nome do seu servidor:

Name: BeAnalytic_DB

<img width="699" height="547" alt="image" src="https://github.com/user-attachments/assets/5e6698ad-e107-4a3c-9c08-a852a464937d" />

Clique na aba 'Connection' e preencha as seguintes informações:

Host: db 
Port: 5432 
Username: postgres 
Password: 12345
-> Habilite o 'Save password'

<img width="700" height="544" alt="image" src="https://github.com/user-attachments/assets/5b0fcad9-1b34-4d18-8fb3-c63bd475c499" />

Com isso, você já conseguirá visualizar o banco de dados e as tabelas existentes dentro do servidor.

Agora você precisará abrir um Query Tool da sua tabela. Basta clicar na tabela no lado esquerdo da tela e clicar em 'Query Tool'

<img width="1869" height="682" alt="image" src="https://github.com/user-attachments/assets/c269a1cf-33be-458a-926e-297a625cb1e7" />


# Querys e criação de views no banco de dados

Agora chegamos no ponto principal, cole este código abaixo para criar uma view com as informações que queremos e fazer a consulta da mesma.

`````
CREATE OR REPLACE VIEW consolidacao_de_metricas AS
WITH taxas_por_grupo AS (
    -- CTE 1: Calcula a taxa de variação para cada grupo econômico
    -- Tabela temporária que será usada depois
    SELECT 
        mes_referencia,
        grupo_economico,
        ROUND(
            -- Fórmula: ((valor_atual - valor_anterior) / valor_anterior) * 100
            -- NULLIF(valor_anterior, 0) evita divisão por zero (retorna NULL se valor_anterior = 0)
            ((valor_atual - valor_anterior) / NULLIF(valor_anterior, 0) * 100)::numeric, -- Converte para numeric porque PostgreSQL não permite ROUND() com casas decimais em double precision (que o python trouxe)
            1  -- Arredonda para 1 casa decimal
        ) AS taxa_var
    FROM (
        -- Subquery para preparar os dados base para cálculo
        -- Calcula a média atual e obtém a média do mês anterior para cada grupo
        SELECT 
            mes_referencia,
            grupo_economico,
            AVG(valor)::numeric AS valor_atual, -- Média do IDA para o mês atual
            LAG(AVG(valor)::numeric) OVER ( -- LAG(): Pega o valor do mês anterior
                PARTITION BY grupo_economico  -- PARTITION BY grupo_economico: Separa o cálculo por cada grupo
                ORDER BY mes_referencia -- ORDER BY mes_referencia: Ordena por data para pegar o mês anterior correto
            ) AS valor_anterior
        FROM be_analytic_table
        WHERE servico = 'Indicador de Desempenho no Atendimento (IDA)' -- Filtra registros específicos
        GROUP BY mes_referencia, grupo_economico -- Agrupa por mês e grupo econômico para calcular médias
    ) t  -- Alias 't' para a subquery
	
    
    WHERE valor_anterior IS NOT NULL -- Filtra apenas linhas onde temos valor do mês anterior 
      AND valor_anterior != 0 -- E onde o valor anterior não é zero
)


SELECT -- Monta o resultado final da view
    
    TO_CHAR(mes_referencia, 'YYYY-MM') AS "Mes", -- Coluna 1: Mês formatado como 'YYYY-MM' (ex: '2019-04')
    
    -- Coluna 2: Taxa de Variação Média (média de todos os grupos)
    
    ROUND(AVG(taxa_var), 1) AS "Taxa de Variação Média (%)", -- Calcula a média das taxas de todos os grupos no mês
    
    -- Colunas 3-8: Valores pivotados de cada grupo econômico
    -- Cada CASE WHEN filtra um grupo específico
    -- COALESCE(..., 0.0): Substitui NULL por 0.0 quando o grupo não tem dados no mês
    
    -- Taxa de variação do grupo ALGAR
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'ALGAR' THEN taxa_var END), 1), 0.0) AS "ALGAR",
    
    -- Taxa de variação do grupo CLARO
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'CLARO' THEN taxa_var END), 1), 0.0) AS "CLARO",
    
    -- Taxa de variação do grupo OI
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'OI' THEN taxa_var END), 1), 0.0) AS "OI",
    
    -- Taxa de variação do grupo TIM
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'TIM' THEN taxa_var END), 1), 0.0) AS "TIM",
    
    -- Taxa de variação do grupo VIVO
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'VIVO' THEN taxa_var END), 1), 0.0) AS "VIVO",
    
    -- Taxa de variação do grupo NEXTEL
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'NEXTEL' THEN taxa_var END), 1), 0.0) AS "NEXTEL",
    
    -- Colunas 9-14: Diferenças entre cada grupo e a média
    -- Fórmula: (taxa do grupo) - (taxa média)
    
    -- Quanto o ALGAR difere da média
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'ALGAR' THEN taxa_var END), 1), 0.0) - ROUND(AVG(taxa_var), 1) AS "ALGAR_Diff",
    
    -- Quanto o CLARO difere da média
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'CLARO' THEN taxa_var END), 1), 0.0) - ROUND(AVG(taxa_var), 1) AS "CLARO_Diff",
    
    -- Quanto o OI difere da média
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'OI' THEN taxa_var END), 1), 0.0) - ROUND(AVG(taxa_var), 1) AS "OI_Diff",
    
    -- Quanto o TIM difere da média
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'TIM' THEN taxa_var END), 1), 0.0) - ROUND(AVG(taxa_var), 1) AS "TIM_Diff",
    
    -- Quanto o VIVO difere da média
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'VIVO' THEN taxa_var END), 1), 0.0) - ROUND(AVG(taxa_var), 1) AS "VIVO_Diff",
    
    -- Quanto o NEXTEL difere da média
    COALESCE(ROUND(AVG(CASE WHEN grupo_economico = 'NEXTEL' THEN taxa_var END), 1), 0.0) - ROUND(AVG(taxa_var), 1) AS "NEXTEL_Diff"

FROM taxas_por_grupo  -- Dados vindo do CTE definido acima
WHERE taxa_var IS NOT NULL -- Filtra apenas taxas não nulas
GROUP BY mes_referencia -- Agrupa os resultados por mês (cada linha = um mês)

-- Filtra meses que tenham pelo menos 2 grupos com dados
-- Evita mostrar meses com apenas um grupo
HAVING COUNT(DISTINCT grupo_economico) >= 2

ORDER BY mes_referencia DESC; -- Ordena do mês mais recente para o mais antigo


-- Mostrar o resultado final
SELECT *
FROM consolidacao_de_metricas
`````

# FIM DO PROJETO
