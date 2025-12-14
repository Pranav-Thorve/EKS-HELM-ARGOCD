FROM node:20-alpine

WORKDIR /app

COPY package.json .
RUN npm install --only=production

COPY db-service.js .

CMD ["node", "db-service.js"]
