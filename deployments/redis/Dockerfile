FROM redis:7.2-alpine

# BUILD_ENV is passed by Compose overlays to document which environment image was built.
ARG BUILD_ENV=development
ENV BUILD_ENV=${BUILD_ENV}

# Keep both redis configs in-image so each build is self-contained.
COPY config/redis.development.conf /etc/redis/redis.development.conf
COPY config/redis.production.conf /etc/redis/redis.production.conf

# Start Redis with the environment-selected config; overlays inject --requirepass at runtime.
ENTRYPOINT ["sh", "-c", "redis-server /etc/redis/redis.${BUILD_ENV}.conf"]

# Documented Redis port.
EXPOSE 6379
