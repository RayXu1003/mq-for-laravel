<?php

namespace PatPat\MessageQueue\Drivers;

use PatPat\MessageQueue\Contracts\MessageQueueContract;
use PatPat\MessageQueue\Traits\AMQPQueueTrait;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * AMQP Message Queue(RabbitMQ)
 */
class AMQP extends MessageQueueContract
{
    use AMQPQueueTrait;

    private $lastMessages = [];
    private $lastMessage = '';

    public function __construct(AMQPStreamConnection $connection, array $config)
    {
        $this->connection       = $connection;
        $this->defaultQueue     = $config['queue'];
        $this->defaultExchange  = $config['default_exchange'] ?: $this->defaultQueue;
        $this->queueParameters  = $config['queue_params'];
        $this->queueArguments   = isset($this->queueParameters['arguments']) ? json_decode($this->queueParameters['arguments'], true) : [];
        $this->configExchange   = $config['exchange_params'];
        $this->declareExchange  = $config['exchange_declare'];
        $this->declareBindQueue = $config['queue_declare_bind'];
        $this->sleepOnError     = $config['sleep_on_error'] ?: 5;

        $this->routeMap = $config['route'];
        if (!empty($this->defaultExchange) && empty($this->routeMap[$this->defaultExchange])) {
            $this->routeMap[$this->defaultExchange] = $this->defaultQueue;
        }

        $this->channel = $this->getChannel();
    }

    public function push($message, $exchange = null, $option=[])
    {dd(2222);
        return $this->pushRaw($message, $exchange, $option);
    }

    public function later($delay, $message, $exchange = null, $option=[])
    {
        $option['delay'] = $this->secondsUntil($delay);
        return $this->pushRaw($message, $exchange, $option);
    }

    public function pop($queue = null, $option=[])
    {
        $queue = $this->getQueueName($queue);
        $route = $this->getRouteByQueue($queue);

        // declare queue if not exists

        $this->declareQueue($route);

        // get envelope
        $message = $this->channel->basic_get($queue);

        if ($message instanceof AMQPMessage) {
            $this->lastMessages[$queue] = $message;
            $this->lastMessage = $message;
            return $message->getBody();
        } else {
            $this->lastMessages[$queue] = null;
            $this->lastMessage = null;
        }

        return null;
    }

    /**
     * 消息ack
     *
     * @param string $queue
     * @param null $message
     * @throws \Exception
     */
    public function ack($queue='', $message = null)
    {
        // 如果指定Message，则Ack该message
        if ($message) {
            $delivery_tag = $message->delivery_info['delivery_tag'];
            $this->channel->basic_ack($delivery_tag);
        } else {
            $lastMessage = $this->lastMessage($queue);
            if ($lastMessage) {
                $this->channel->basic_ack($lastMessage->delivery_info['delivery_tag']);
            } else {
                throw new \Exception('无效的delivery_tag，ack失败');
            }
        }
    }

    public function reject($queue='', $message = null)
    {
        // 如果指定Message，则Ack该message
        if ($message) {
            $delivery_tag = $message->delivery_info['delivery_tag'];
            $this->channel->basic_nack($delivery_tag);
        } else {
            $lastMessage = $this->lastMessage($queue);
            if ($lastMessage) {
                $this->channel->basic_nack($lastMessage->delivery_info['delivery_tag']);
            } else {
                throw new \Exception('无效的delivery_tag，ack失败');
            }
        }
    }

    public function requeue($queue='', $message = null)
    {
        // 如果指定Message，则Ack该message
        if ($message) {
            $delivery_tag = $message->delivery_info['delivery_tag'];
            $this->channel->basic_nack($delivery_tag, false, true);
        } else {
            $lastMessage = $this->lastMessage($queue);
            if ($lastMessage) {
                $this->channel->basic_nack($lastMessage->delivery_info['delivery_tag'], false, true);
            } else {
                throw new \Exception('无效的delivery_tag，ack失败');
            }
        }
    }

    /**
     * 使用指定闭包回调函数来处理输出
     * consume函数是在用于被动接受数据来消费，因而需要回调函数
     * 使用该机制比主动使用basic_get获取数据的速度要快很多
     *
     * @param $callback
     * @param null $queue
     * @param bool $is_ack
     * @param array $option
     * @return $this
     */
    public function consume($callback, $queue = null, $is_ack = true, $option=[])
    {
        $consumerTag = $this->getConsumerTag();
        $queue = $this->getQueueName($queue);

        $this->channel->basic_consume($queue, $consumerTag, false, false, false, false, function($message) use ($callback, $is_ack) {

            $data = $message->getBody();
            $result = call_user_func($callback, $data);

            if ($is_ack && $result) {
                $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
            }

        });
        return $this;
    }

    /**
     * 启动消费
     *
     * @param int $limit
     */
    public function start($limit=0)
    {
        $i = 0;
        while (count($this->channel->callbacks)) {
            $i++;
            if ($limit>0 && $i>$limit) break;
            $this->channel->wait();
        }
    }

    /**
     * 获取最后一条信息
     *
     * @param string $queue
     * @return mixed|string|null
     */
    public function lastMessage($queue='')
    {

        $queue = $this->getQueueName($queue);
        $lastMessage = !empty($this->lastMessages[$queue]) ? $this->lastMessages[$queue] : null;
        if (empty($lastMessage) && empty($queue)) {
            $lastMessage = $this->lastMessage;
        }

        return $lastMessage;
    }

    public function getLastAckMark($queue='')
    {
        $message = $this->lastMessage($queue);
        return $message ? $message->delivery_info['delivery_tag'] : null;
    }

    public function close()
    {
        // $this->channel->basic_cancel($this->getConsumerTag(), false, true);
        if (!is_null($this->channel)) $this->channel->close();
        $this->channel = null;
        $this->connection->close();
    }

    public function reconnect()
    {
        $this->connection->reconnect();
        $this->channel = $this->getChannel();
    }
}
