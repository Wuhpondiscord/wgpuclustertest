FROM node:22-alpine AS frontend
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY index.html tsconfig.json tsconfig.app.json vite.config.ts ./
COPY src ./src
RUN npm run build

FROM rust:1.99.1-slim AS backend
WORKDIR /app
COPY server/Cargo.toml server/Cargo.lock ./
COPY server/src ./src
RUN cargo build --release --locked --bin mesh-coordinator

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 --create-home app
WORKDIR /app
COPY --from=backend /app/target/release/mesh-coordinator /usr/local/bin/mesh-coordinator
COPY --from=frontend /app/dist ./dist
ENV PORT=7860
ENV MESH_REQUIRE_GITHUB_STORAGE=1
EXPOSE 7860
USER app
CMD ["/usr/local/bin/mesh-coordinator"]
