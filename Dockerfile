# ... (stages anteriores inalterados)

FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

# deps mínimas
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# copia backend
COPY backend /app/backend

# copia build do front para o static do back (do estágio de frontend)
RUN rm -rf /app/backend/static/dashboard && mkdir -p /app/backend/static/dashboard
COPY --from=frontend /app/frontend/dist/ /app/backend/static/dashboard/

# instala deps
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /app/backend/requirements.txt

# cria/atualiza o DB; se falhar, não derruba o container
RUN python /app/backend/scripts/build_database.py || echo "WARN: build_database falhou; seguindo"

EXPOSE 8000

# >>> use o caminho de módulo totalmente qualificado
CMD ["bash", "-lc", "python -m uvicorn backend.app:app --host 0.0.0.0 --port ${PORT:-8000}"]
