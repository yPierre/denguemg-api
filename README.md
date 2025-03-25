# Atualização de Dados do Observatório de Dengue

Olá! Este repositório contém o script que mantém o *dashboard* de dengue em Minas Gerais sempre atualizado com os dados da API do InfoDengue. Ele é parte do meu TCC.

## O que ele faz?
- Consulta a API pública do InfoDengue semanalmente;
- Processa os dados e atualiza o banco MongoDB Atlas;
- Garante que o *dashboard* tenha informações atualizadas toda segunda-feira.

## Tecnologias usadas
- JavaScript com Express
- MongoDB Atlas
- Hospedagem: Render
- Agendamento: fastcron.com
