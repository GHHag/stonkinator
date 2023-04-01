FROM node:bullseye-slim

WORKDIR /app

COPY ["package.json", "package-lock.json*", "./"]

RUN npm install

COPY . .

EXPOSE 4000

CMD ["node", "api/server.js"]
