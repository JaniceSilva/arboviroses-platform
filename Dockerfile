# ------------ STAGE 1: build do frontend (Vite/React) ------------
FROM node:20-alpine AS frontend
WORKDIR /app/frontend

# Instala só o necessário antes de copiar o resto (cache eficiente)
COPY frontend/package*.json ./
RUN npm ci

# Copia o código do front e faz o build com base em /static/dashboard/
COPY frontend ./
RUN npm run build -- --base=/static/dashboard/

# ------------ STAGE 2: backend Python + assets estáticos ------------
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# (Opcional) dependências de sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copia o backend
COPY backend /app/backend

# Copia o build do front para o static do backend
RUN rm -rf /app/backend/static/dashboard && mkdir -p /app/backend/static/dashboard
COPY --from=frontend /app/frontend/dist/ /app/backend/static/dashboard/

# Instala deps Python
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /app/backend/requirements.txt

EXPOSE 8000

# Sobe criando/atualizando o banco e iniciando a API
# Render define $PORT em runtime; localmente cai no 8000.
CMD bash -lc "cd /app/backend && python scripts/build_database.py && uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"  
