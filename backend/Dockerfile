FROM python:3.11-slim

WORKDIR /app

# Static Docker CLI (client only, no daemon) so the backend can `docker cp` /
# `docker exec` the generated PyFlink job into the Flink JobManager container via
# the mounted socket (see services/job_launcher.py). Downloaded with Python since
# the slim image ships no curl/wget; arch-matched via TARGETARCH.
ARG TARGETARCH
RUN python3 -c "import urllib.request; a={'amd64':'x86_64','arm64':'aarch64'}.get('${TARGETARCH:-amd64}','x86_64'); urllib.request.urlretrieve(f'https://download.docker.com/linux/static/stable/{a}/docker-27.3.1.tgz','/tmp/d.tgz')" \
 && tar -xzf /tmp/d.tgz -C /tmp \
 && mv /tmp/docker/docker /usr/local/bin/docker \
 && rm -rf /tmp/d.tgz /tmp/docker \
 && docker --version

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
