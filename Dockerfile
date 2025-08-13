# STAGE 1: FRONT
FROM node:20-alpine AS frontend
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend ./
RUN npm run build             # usa o base do vite.config.js

# STAGE 2: BACK
FROM python:3.11-slim
WORKDIR /app
COPY backend /app/backend
# copia o build do front para o static do back
RUN rm -rf /app/backend/static/dashboard && mkdir -p /app/backend/static/dashboard
COPY --from=frontend /app/frontend/dist/ /app/backend/static/dashboard/
RUN pip install --no-cache-dir -r /app/backend/requirements.txt
EXPOSE 8000
CMD bash -lc "cd /app/backend && python scripts/build_database.py && uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"
