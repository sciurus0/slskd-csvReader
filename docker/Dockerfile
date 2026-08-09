# csvReader web UI — Python only (static assets are plain HTML/CSS/JS).
# NAS build installs from vendored wheels/ (no PyPI on Synology).

FROM python:3.12-slim
WORKDIR /app

COPY docker/requirements.txt ./docker/
COPY docker/wheels ./docker/wheels/
RUN pip install --no-index --find-links=./docker/wheels -r docker/requirements.txt

# Flat script layout (same as repo root used on Mac)
COPY *.py ./
COPY webapp ./webapp
COPY config.ini.example ./

ENV PYTHONUNBUFFERED=1
ENV CSVREADER_WORKSPACE=/data
ENV CSVREADER_CONFIG_FILE=/data/config.ini
ENV SPOTIFY_TOKEN_CACHE=/data/spotify_tokens.json

EXPOSE 8766
CMD ["python", "-m", "uvicorn", "webapp.app:app", "--host", "0.0.0.0", "--port", "8766"]
