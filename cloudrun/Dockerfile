FROM python:3.11-slim

# Playwright (任意：必要な場合のみ)
RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates fonts-liberation libnss3 libatk-bridge2.0-0 libxkbcommon0 \
    libxcomposite1 libxrandr2 libxi6 libxcursor1 libxdamage1 libasound2 libpangocairo-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY cloudrun/requirements.txt /app/requirements.txt
RUN pip install -U pip && pip install -r requirements.txt

COPY . /app
CMD ["python","cloudrun/runner.py"]
