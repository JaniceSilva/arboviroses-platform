# ------------ STAGE 1: build do frontend (Vite/React) ------------
FROM node:20-alpine AS frontend
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend ./
# base configurada no vite.config.js como /static/dashboard/
RUN npm run build

# ------------ STAGE 2: backend Python + assets estáticos ------------
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# deps mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# copia backend
COPY backend /app/backend

# copia build do front para static do back
RUN rm -rf /app/backend/static/dashboard && mkdir -p /app/backend/static/dashboard
COPY --from=frontend /app/frontend/dist/ /app/backend/static/dashboard/

# instala deps Python
WORKDIR /app/backend
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# >>> GERA O BANCO NO BUILD (falha não derruba a imagem)
RUN python scripts/build_database.py || echo "WARNING: DB build falhou; prosseguindo"

EXPOSE 8000

# inicia uvicorn no PORT do Render
CMD uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}
