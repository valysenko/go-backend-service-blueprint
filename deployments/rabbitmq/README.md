The bundled `rabbitmq_delayed_message_exchange-4.1.0.ez` is the unmodified
[official v4.1.0 release](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/tag/v4.1.0)
of the RabbitMQ delayed-message exchange plugin (MPL-2.0). The Dockerfile checks
its SHA-256 before enabling it, so the image build does not download the plugin.
