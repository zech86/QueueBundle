<?php

namespace Zechim\QueueBundle\Service\Producer;

class CommandProducer extends AbstractProducer
{
    public function publish($name, array $options = [], \DateInterval $interval = null)
    {
        $this->doPublish(['command' => $name, 'options' => $options], $interval);
    }
}
