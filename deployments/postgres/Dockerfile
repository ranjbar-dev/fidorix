FROM timescale/timescaledb:latest-pg16

# BUILD_ENV is passed by Compose overlays to document which environment image was built.
ARG BUILD_ENV=development
ENV BUILD_ENV=${BUILD_ENV}

# Keep both postgres configs in-image so each build is self-contained.
COPY config/postgresql.development.conf /etc/postgresql/postgresql.development.conf
COPY config/postgresql.production.conf /etc/postgresql/postgresql.production.conf

# Initialize extension, app role, grants, and schemas on first database bootstrap.
COPY init.sql /docker-entrypoint-initdb.d/10-init.sql

# Documented PostgreSQL port.
EXPOSE 5432
