FROM node:20-alpine
WORKDIR /app
COPY package.json .
RUN npm install
COPY . .
CMD ["npx", "nodemon", "-L", "--watch", "src", "src/index.js"]