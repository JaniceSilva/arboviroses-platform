# Arboviroses Platform — Final (ready for GitHub + Render)

Este repositório contém o sistema pronto para produção (prototype -> prod) com:
- Backend (FastAPI) + ETL scripts para OpenWeather, INMET e DATASUS/SINAN.
- Treinamento LSTM (TensorFlow) com split temporal e avaliação.
- Frontend (React + Vite) com dashboard estilo InfoDengue.
- Docker + docker-compose e instruções para deploy no Render via integração GitHub.
- GitHub Actions workflow template e instruções para configurar segredos (OPENWEATHER_KEY e RENDER_*).

## Passos rápidos
1. Crie um repositório no GitHub e faça push deste projeto.
2. No GitHub, vá em Settings > Secrets e crie `OPENWEATHER_KEY` (sua chave da OpenWeather).
3. Conecte o repositório ao Render (Web UI) — Render detecta Dockerfile e fará deploy automático em cada push.
4. Opcional: adicionar `RENDER_API_KEY` e `RENDER_SERVICE_ID` se quiser deploy via GitHub Actions (workflow incluso).

## Dados
- As rotinas ETL automatizam downloads de: OpenWeather (History/Current), INMET (BDMEP), e DATASUS/SINAN.
- Para SINAN/DATASUS eu uso abordagens compatíveis com o pacote `microdatasus`/`pysus` (ver exemplos em scripts).

## Segurança
- NÃO coloque chaves (API keys) em código. Use sempre GitHub Secrets e variáveis de ambiente no Render.
- Após inserir a `OPENWEATHER_KEY` no GitHub, recomendo revogar a chave que foi exposta no chat por segurança.

## Estrutura
- backend/: FastAPI, ETL scripts, model training.
- frontend/: React + Vite dashboard
- .github/workflows/: CI template (build/test + optional Render deploy)
- docker-compose.yml, README.md

## Observações técnicas / fontes
- OpenWeather APIs (current, forecast, history) — necessário `OPENWEATHER_KEY` para chamadas History e Current.
- INMET BDMEP — portal oficial de dados meteorológicos do INMET.
- DATASUS / SINAN — portais oficiais e pacotes `microdatasus` / `pysus` são recomendados para baixar microdados.

