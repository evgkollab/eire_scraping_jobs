

FROM python:3.11-slim

# --- 1. Install system deps & Chrome/Chromedriver --------------------
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        chromium \
        chromium-driver \
        fonts-liberation \
        libasound2 \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libcups2 \
        libdbus-1-3 \
        libgdk-pixbuf-2.0-0 \
        libnspr4 \
        libnss3 \
        libx11-xcb1 \
        libxcomposite1 \
        libxdamage1 \
        libxrandr2 \
        xdg-utils \
        ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Tell script where Chrome & Chromedriver live
ENV GOOGLE_CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# --- 2. Create app directory ----------------------------------------
WORKDIR /app

# --- 3. Copy requirements & install ---------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- 4. Copy source code --------------------------------------------
COPY . .

ENTRYPOINT ["python", "-u", "main.py"]
