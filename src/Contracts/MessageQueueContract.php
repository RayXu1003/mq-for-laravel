<?php

namespace Rex\MessageQueue\Contracts;

use Closure;
use DateInterval;
use DateTimeInterface;

/**
 * 消息队列接口
 */
abstract class MessageQueueContract
{

    protected $connection = null;
    /**
     * 默认队列名称
     * @var string
     */
    protected $defaultQueue = null;
    /**
     * 连接名称
     * @var string
     */
    protected $connectionName = null;

    /**
     * 获取当前队列长度
     */
    abstract public function size($queue = null);

    /**
     * push一条消息
     */
    abstract public function push($message, $routing_key = '', $exchange = null, $option = []);

    /**
     * push一条延时消息
     */
    abstract public function delay($delay, $message, $queue = null, $option=[]);

    /**
     * 弹出一条消息
     */
    abstract public function pop($queue = null);

    /**
     * 指定闭包回调函数来消费队列
     */
    abstract public function consume($queue = null, $is_ack = true, $callback);

    /**
     * 和consume配合使用，启动消费
     */
    abstract public function start($limit=0);

    /**
     * 删除消息
     * Ack
     */
    abstract public function ack($queue='', $message = null);

    /** 拒绝消息 */
    abstract public function reject($queue='', $message = null);

    /** 消息重新归队 */
    abstract public function requeue($queue='', $message = null);

    /**
     * 关闭队列和连接
     */
    abstract public function close();

    /**
     * 队列重连
     */
    abstract public function reconnect();

    /**
     * 设置队列
     */
    public function setQueue($queue = null)
    {
        if ($queue) $this->defaultQueue = $queue;

        return $this->defaultQueue;
    }

    /**
     * 获取队列名称
     */
    public function getQueueName($queue = null)
    {
        return $queue ?: $this->defaultQueue;
    }

    /**
     * 获取连接名称
     */
    public function getConnectionName()
    {
        return $this->connectionName;
    }

    /**
     * 设置连接名称
     */
    public function setConnectionName($name)
    {
        $this->connectionName = $name;
        return $this;
    }

    /**
     * 获取当前连接
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * Set the IoC container instance.
     *
     * @param  \Illuminate\Container\Container  $container
     * @return void
     */
    public function setContainer($container)
    {
        $this->container = $container;
    }

    protected function secondsUntil($delay)
    {
        return (int) $delay;
    }
}
