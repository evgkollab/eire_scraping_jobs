FROM python:3.12-slim

# --- 1. Install system deps & Google Chrome Stable --------------------
ENV DEBIAN_FRONTEND=noninteractive

# Install basic deps + wget/gnupg to fetch Chrome
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    ca-certificates \
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
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome Stable (Official)
# ⬇️ CHANGED: Use direct download instead of apt-key
RUN wget -q -O /etc/apt/trusted.gpg.d/google.asc https://dl-ssl.google.com/linux/linux_signing_key.pub \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' \
    && apt-get update \
    && apt-get install -y google-chrome-stable --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Set the binary location for your Python script
ENV GOOGLE_CHROME_BIN=/usr/bin/google-chrome

# --- 2. Create app directory ----------------------------------------
WORKDIR /app

# --- 3. Copy requirements & install ---------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- 4. Copy source code --------------------------------------------
COPY . .

ENTRYPOINT ["python", "-u", "main.py"]
