FROM node:20-alpine
WORKDIR /app
COPY package.json .
RUN npm install
COPY . .
# nodemon watches /app/src which is bind-mounted from the host
CMD ["npx", "nodemon", "--watch", "src", "src/index.js"]