FROM postgres:17-alpine AS postgres

# set working directory in container
WORKDIR /usr/src/database_service

# copy SQL setup script into entrypoint directory
COPY db_setup.sql /docker-entrypoint-initdb.d/

# expose PostgreSQL port
EXPOSE 5432