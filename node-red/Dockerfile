# Official maintained Node-RED 4.x release, Node.js 22, multi-architecture.
# Tag verified in the publisher's Docker Hub registry on 2026-09-06.
FROM nodered/node-red:4.1.14-22
USER root
RUN mkdir -p /app/src && chown -R node-red:node-red /app /data
COPY --chown=node-red:node-red src/domain.cjs /app/src/domain.cjs
COPY --chown=node-red:node-red node-red/settings.cjs /data/settings.cjs
COPY --chown=node-red:node-red node-red/flows.json /data/flows.json
USER node-red
ENV MQTT_HOST=mosquitto
EXPOSE 1880
CMD ["--settings", "/data/settings.cjs", "/data/flows.json"]
