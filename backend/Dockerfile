FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install --only=production

COPY backend.js .

EXPOSE 7901

CMD ["node", "backend.js"]
