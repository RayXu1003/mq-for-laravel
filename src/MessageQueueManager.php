<?php

namespace Rex\MessageQueue;

use Rex\MessageQueue\Drivers\AMQP;
use InvalidArgumentException;
use Closure;

/**
 * MessageQueueManager
 */
class MessageQueueManager
{

    protected $app;

    public function __construct($app)
    {
        $this->app = $app;
    }

    /**
     * 获取消息队列连接
     *
     * @param null $name
     * @return AMQP
     */
    public function connection($name = null): AMQP
    {
        $name = $name ?: $this->getDefaultDriver();

        if (!isset($this->connections[$name])) {
            $this->connections[$name] = $this->resolve($name);
            $this->connections[$name]->setContainer($this->app);
        }

        return $this->connections[$name];
    }

    /**
     * 默认
     *
     * @return mixed
     */
    public function getDefaultDriver()
    {
        return $this->app['config']['mq.default'];
    }

    /**
     *
     * @param null $name
     * @return mixed
     */
    public function resolve($name = null)
    {
        $config = $this->getConfig($name);

        return $this->getConnector($config['driver'])
            ->connect($config)
            ->setConnectionName($name);
    }

    protected function getConnector($driver)
    {
        if (!isset($this->connectors[$driver])) {
            throw new InvalidArgumentException("No amqp connector for [$driver]");
        }

        return call_user_func($this->connectors[$driver]);
    }

    public function extend($driver, Closure $resolver)
    {
        return $this->addConnector($driver, $resolver);
    }

    public function addConnector($driver, Closure $resolver)
    {
        $this->connectors[$driver] = $resolver;
    }

    protected function getConfig($name)
    {
        if (!is_null($name) && $name !== 'null') {
            return $this->app['config']["mq.connections.{$name}"];
        }

        return ['driver' => 'null'];
    }

    public function __call($method, $parameters)
    {
        return $this->connection()->$method(...$parameters);
    }
}

