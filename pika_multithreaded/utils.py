class AmqpUtils:
    @staticmethod
    def generate_url(host, port=None, user=None, password=None, use_ssl=False):
        # Make sure the port number is set correctly
        if not port:
            if use_ssl:
                port = 5671
            else:
                port = 5672
        # Set the credentials if a user and password are passed in
        if user and password:
            credentials = f"{user}:{password}"
        else:
            credentials = ""
        # Return the generated URL
        url = "amqps://" if use_ssl else "amqp://"
        url += f"{credentials}@" if credentials else ""
        url += f"{host}:{port}"
        return url

    @staticmethod
    def generate_amazon_mq_url(broker_id, user, password, region, port=None, use_ssl=True):
        # Return the URL from the base "generate_url" function by setting the "host" to the
        # Amazon MQ host name
        return AmqpUtils.generate_url(
            host=f"{broker_id}.mq.{region}.amazonaws.com",
            port=port,
            user=user,
            password=password,
            use_ssl=use_ssl
        )
