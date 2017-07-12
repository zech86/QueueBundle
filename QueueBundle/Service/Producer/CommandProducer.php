<?php

namespace Zechim\QueueBundle\Service\Producer;

class CommandProducer extends AbstractProducer
{
    public function publish($routingKey, $name, array $options = [])
    {
        $this->doPublish(
            [
                'command' => $name,
                'options' => $options
            ],
            $routingKey
        );
    }
}
