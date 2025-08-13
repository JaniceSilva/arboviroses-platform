# ---------- STAGE 1: build do frontend ----------
FROM node:20-alpine AS frontend
WORKDIR /app/frontend

# Instala com cache eficiente
COPY frontend/package*.json ./
RUN npm ci --no-audit --no-fund

# Copia o código e faz build
COPY frontend ./
# Mostra logs mais verbosos do Vite
ENV NODE_ENV=production
RUN npm run build -- --debug

# ---------- STAGE 2: backend + assets ----------
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependências mínimas do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copia backend
COPY backend /app/backend

# Copia build do front para static do back
RUN rm -rf /app/backend/static/dashboard && mkdir -p /app/backend/static/dashboard
COPY --from=frontend /app/frontend/dist/ /app/backend/static/dashboard/

# Instala deps Python
WORKDIR /app/backend
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Gera DB no build (se falhar, não quebra a imagem)
RUN python scripts/build_database.py || echo "WARN: build_database falhou; seguindo"

EXPOSE 8000
CMD uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}

CMD bash -lc "cd /app/backend \
  && python scripts/build_database.py || echo 'WARN: build_database falhou; seguindo' \
  && uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"

