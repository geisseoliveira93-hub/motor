FROM node:22-bookworm-slim
WORKDIR /app
# better-sqlite3 precisa de build tools no install
RUN apt-get update && apt-get install -y --no-install-recommends python3 make g++ && rm -rf /var/lib/apt/lists/*
# curl/unzip/ca-certificates + rclone (OneDrive) + ffmpeg (etapa33: lives "ao vivo gravado")
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates ffmpeg && curl -fsSL -o /tmp/rc.zip https://downloads.rclone.org/rclone-current-linux-amd64.zip && unzip -j /tmp/rc.zip -d /tmp/rc && mv /tmp/rc/rclone /usr/local/bin/rclone && chmod 755 /usr/local/bin/rclone && rm -rf /tmp/rc.zip /tmp/rc && rm -rf /var/lib/apt/lists/*
COPY package.json ./
RUN npm install --no-audit --no-fund || npm install --no-audit --no-fund
# [+yt-browser-nuvem 28/10] Playwright + Chromium pro "navegador invisível" do YouTube na nuvem.
# Instala o playwright LOCAL (em /app/node_modules, pra o require('playwright') do index.ts achar)
# e baixa o Chromium + libs do sistema (--with-deps). PLAYWRIGHT_BROWSERS_PATH fixo pra o runtime
# achar o browser no mesmo lugar. Se o download do Chromium falhar no build, o deploy-motor-v2.sh
# mantém o container antigo de pé (nada quebra em produção).
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN npm install --no-audit --no-fund playwright@1.49.1 && npx playwright install --with-deps chromium
COPY tsconfig.json ./
COPY src ./src
ENV NODE_ENV=production
EXPOSE 8080
CMD ["npx", "tsx", "src/index.ts"]
