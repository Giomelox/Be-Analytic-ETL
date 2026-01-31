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
