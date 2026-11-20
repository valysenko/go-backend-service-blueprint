FROM rabbitmq:4.1-management-alpine

# Vendored from the official v4.1.0 release so tests do not depend on GitHub
# availability at image-build time. Update the artifact and hash together.
COPY rabbitmq_delayed_message_exchange-4.1.0.ez /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-4.1.0.ez
RUN echo '567f876378e70af9d949de4066bbb2fc30162b46bcbc9efe2430076b172e4a87  /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-4.1.0.ez' | sha256sum -c - \
    && rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange
