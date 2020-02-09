# Dockerfile for kiwi bulk loader
FROM node:13
MAINTAINER Blake Regalia <blake.regalia@gmail.com>

# source code
WORKDIR /src/app
COPY . .

# add PostgreSQL keys
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main" > /etc/apt/sources.list.d/pgdg.list

# install packages
RUN apt-get -y update \
    && apt-get upgrade -y \
    && apt-get install -yq \
        postgresql-client-common \
        postgresql-client-12.1 \
    && apt-get clean

# install software
RUN npm i

# entrypoint
ENTRYPOINT ["npm", "run"]
CMD ["all"]
