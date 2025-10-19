FROM node:20-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci --omit=dev

COPY . .

ENV NODE_ENV=production \
    PORT=3005

EXPOSE 3005

CMD ["npm", "start"]
