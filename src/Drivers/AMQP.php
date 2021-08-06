<?php

namespace Rex\MessageQueue\Drivers;

use Rex\MessageQueue\Contracts\MessageQueueContract;
use Rex\MessageQueue\Traits\AMQPQueueTrait;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * AMQP Message Queue(RabbitMQ)
 */
class AMQP extends MessageQueueContract
{
    use AMQPQueueTrait;

    private $lastMessages = [];
    private $lastMessage = '';

    /**
     * 消息发布模式
     *
     * @var array
     */
    protected static $models = ['default', 'confirm', 'return'];

    /**
     * AMQP constructor.
     *
     * @param AMQPStreamConnection $connection
     * @param array $config
     */
    public function __construct(AMQPStreamConnection $connection, array $config)
    {
        $this->connection       = $connection;
        $this->defaultQueue     = $config['default_queue'];
        $this->defaultExchange  = $config['default_exchange'] ?: $this->defaultQueue;

        // queue声明属性
        $this->queueParams    = $config['queue_params'];
        $this->queueArguments = isset($this->queueParams['arguments']) ? json_decode($this->queueParams['arguments'], true) : [];

        // exchange声明属性
        $this->exchangeParams = $config['exchange_params'];

        // declare
        $this->declareExchange  = $config['exchange_declare'];
        $this->declareBindQueue = $config['queue_declare_bind'];

        $this->sleepOnError = $config['sleep_on_error'] ?: 5;

        // route
        $this->routeMap = $config['route'];
        // queue为空，则取defaultQueue
        if (!empty($this->defaultExchange) && empty($this->routeMap[$this->defaultExchange])) {
            $this->routeMap[$this->defaultExchange] = $this->defaultQueue;
        }

        // bind
        $this->bindingMap = $config['binding'];

        $this->channel = $this->getChannel();

        $this->model = 'default';   // 发布消息模式
    }

    /**
     * 设置消息发布模式
     *
     * @param $model
     */
    public function setModel($model){
        if (in_array($model, self::$models)) {
            $this->model = $model;
            $this->channel->confirm_select();
        }
    }

    public function push($message, $routing_key = '', $exchange = null, $option = []){
        return $this->pushRaw($message, $routing_key, $exchange, $option);
    }

    public function delay($delay, $message, $routing_key = '', $exchange = null, $option = []){
        $option['delay'] = $this->secondsUntil($delay);
        return $this->pushRaw($message, $routing_key, $exchange, $option);
    }

    public function easy_delay($delay, $message, $routing_key = '', $exchange = null, $option = []){
        $option['easy_delay'] = $this->secondsUntil($delay);
        return $this->pushRaw($message, $routing_key, $exchange, $option);
    }

    /**
     * 拉模式
     *
     * @param null $queue
     * @return string|null
     */
    public function pop($queue = null){
        $queue = $this->getQueueName($queue);
        $route = $this->getRouteByQueue($queue);

        // declare queue if not exists
        $this->declareQueue($route);

        // get envelope
        $message = $this->channel->basic_get($queue);
        if ($message instanceof AMQPMessage) {
            $this->lastMessages[$queue] = $message;
            $this->lastMessage = $message;

            return $message;
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
    public function ack($queue = '', $message = null){
        // 如果指定message，则ack该message
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

    public function reject($queue = '', $message = null){
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

    public function requeue($queue = '', $message = null){
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
     * 推模式
     * 使用指定闭包回调函数来处理输出
     * consume函数是在用于被动接受数据来消费，因而需要回调函数
     * 使用该机制比主动使用basic_get获取数据的速度要快很多
     *
     * @param null $queue
     * @param bool $is_ack
     * @param array $callback
     */
    public function consume($queue = null, $is_ack = true, $callback){
        $queue = $this->getQueueName($queue);
        $cTag  = $this->getConsumerTag($queue);

        $this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume($queue, $cTag, false, false, false, false, function($message) use ($callback, $is_ack) {
            $result = call_user_func($callback, $message);
            if ($is_ack && $result) {
                $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
            }
        });
    }

    /**
     * 启动消费
     *
     * @param int $limit 消费个数限制
     */
    public function start($limit = 0){
        $i = 0;
        while (count($this->channel->callbacks)) {
            $i++;
            if ($limit > 0 && $i > $limit) break;

            $this->channel->wait();
        }
    }

    /**
     * 获取最后一条信息
     *
     * @param string $queue
     * @return mixed|string|null
     */
    public function lastMessage($queue = ''){
        $queue = $this->getQueueName($queue);
        $lastMessage = !empty($this->lastMessages[$queue]) ? $this->lastMessages[$queue] : null;
        if (empty($lastMessage) && empty($queue)) {
            $lastMessage = $this->lastMessage;
        }

        return $lastMessage;
    }

    public function getLastAckMark($queue = ''){
        $message = $this->lastMessage($queue);
        return $message ? $message->delivery_info['delivery_tag'] : null;
    }

    public function close(){
        // $this->channel->basic_cancel($this->getConsumerTag(), false, true);
        if (!is_null($this->channel)) $this->channel->close();
        $this->channel = null;
        $this->connection->close();
    }

    public function reconnect(){
        $this->connection->reconnect();
        $this->channel = $this->getChannel();
    }
}
