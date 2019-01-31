# Dockerfile for kiwi bulk loader
FROM node:11
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
        postgresql-client-9.5 \
    && apt-get clean

# install software
RUN npm i

# entrypoint
ENTRYPOINT ["npm", "run"]
CMD ["all"]
