<?php

namespace Zechim\QueueBundle\Service\Producer;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

abstract class AbstractProducer
{
    /**
     * @var array
     */
    protected $parameters;

    /**
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * @var AMQPStreamConnection
     */
    protected $connection;

    public function __construct(array $parameters)
    {
        $this->parameters = $parameters;
    }

    /**
     * @param $routingKey
     * @return AMQPChannel
     */
    protected function getChannel($routingKey)
    {
        if (null !== $this->channel) {
            return $this->channel;
        }

        $this->channel = $this->createConnection()->channel();
        $this->channel->queue_declare($routingKey, false, true, false, false);

        return $this->channel;
    }

    /**
     * @return AMQPStreamConnection
     */
    private function createConnection()
    {
        if (null !== $this->connection) {
            return $this->connection;
        }

        return $this->connection = new AMQPStreamConnection(
            $this->parameters['host'],
            $this->parameters['port'],
            $this->parameters['user'],
            $this->parameters['password']
        );
    }

    protected function close()
    {
        if (null !== $this->channel) {
            $this->channel->close();
            $this->channel = null;
        }

        if (null !== $this->connection) {
            $this->connection->close();
            $this->connection = null;
        }
    }

    /**
     * @param array $message
     * @param $routingKey
     */
    protected function doPublish(array $message, $routingKey)
    {
        $message = new AMQPMessage(
            json_encode($message),
            [
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
                'content_type' => 'application/json',
            ]
        );

        $this->getChannel($routingKey)->basic_publish($message, '', $routingKey);
        $this->close();
    }

    abstract function publish($routingKey, $name, array $options = []);
}
