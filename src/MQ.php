<?php

namespace Rex\MessageQueue;

use Rex\MessageQueue\Drivers\AMQP;
use Illuminate\Support\Facades\Facade;


/**
 *
 * @method static AMQP connection(string $name = null)
 *
 * Class MQ
 * @package App\Services\MessageQueue\
 * @see MessageQueueManager
 */
class MQ extends Facade
{
    protected static function getFacadeAccessor()
    {
        return 'mq';
    }
}
