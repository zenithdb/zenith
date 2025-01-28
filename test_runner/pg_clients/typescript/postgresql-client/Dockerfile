FROM node:22@sha256:fa54405993eaa6bab6b6e460f5f3e945a2e2f07942ba31c0e297a7d9c2041f62
WORKDIR /source

COPY . .
RUN npm clean-install

CMD ["/source/index.js"]
