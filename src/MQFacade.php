<?php

namespace RuiXu\MessageQueue;

use RuiXu\MessageQueue\Drivers\AMQP;
use Illuminate\Support\Facades\Facade;


/**
 *
 * @method static AMQP connection(string $name = null)
 *
 * Class MQFacade
 * @package App\Services\MessageQueue\
 * @see MessageQueueManager
 */
class MQFacade extends Facade
{
    protected static function getFacadeAccessor()
    {
        return 'mq';
    }
}
