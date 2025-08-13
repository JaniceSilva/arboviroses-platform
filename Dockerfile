# ---------- STAGE 1: build do frontend (Vite/React) ----------
FROM node:20-alpine AS frontend
WORKDIR /app/frontend

# Instala dependências (use npm install se seu lockfile não está sincronizado)
COPY frontend/package*.json ./
RUN npm install --no-audit --no-fund

# Copia o código e faz o build (vite.config.js deve ter base '/static/dashboard/')
COPY frontend ./
ENV NODE_ENV=production
RUN npm run build -- --debug

# ---------- STAGE 2: backend Python + assets estáticos ----------
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# deps mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copia backend
COPY backend /app/backend

# Copia build do front para o static do back (AGORA o estágio 'frontend' existe)
RUN rm -rf /app/backend/static/dashboard && mkdir -p /app/backend/static/dashboard
COPY --from=frontend /app/frontend/dist/ /app/backend/static/dashboard/

# Instala requirements
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /app/backend/requirements.txt

# (Opcional) Gera o DB no build; se falhar, não derruba imagem
RUN python /app/backend/scripts/build_database.py || echo "WARN: build_database falhou; seguindo"

EXPOSE 8000

# Importa o app pelo caminho de módulo totalmente qualificado
CMD ["bash", "-lc", "python -m uvicorn backend.app:app --host 0.0.0.0 --port ${PORT:-8000}"]
