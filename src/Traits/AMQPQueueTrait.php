<?php

namespace Rex\MessageQueue\Traits;

use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Message\AMQPMessage;
use Illuminate\Support\Facades\Log;

trait AMQPQueueTrait {

    protected $channel;

    protected $declareExchange;
    protected $declareBindQueue;
    protected $sleepOnError;

    protected $queueParams;
    protected $queueArguments;
    protected $exchangeParams;

    private $declaredExchanges = [];
    private $declaredQueues = [];

    private $retryAfter = null;
    private $correlationId = null;

    public function size($queue = null){
        list(, $messageCount) = $this->channel->queue_declare($this->getQueueName($queue), true);

        return $messageCount;
    }

    public function getChannel(){
        return $this->connection->channel();
    }

    private function declareQueue($route){
        foreach ($route as $exchange => $queues) {
            // declare exchange
            $exchange = $this->getExchangeName($exchange);
            if ($this->declareExchange && !in_array($exchange, $this->declaredExchanges, true)) {
                $this->channel->exchange_declare(
                    $exchange,
                    $this->exchangeParams['type'],
                    $this->exchangeParams['passive'],
                    $this->exchangeParams['durable'],
                    $this->exchangeParams['auto_delete']
                );

                $this->declaredExchanges[] = $exchange;
            }

            // declare queue
            $queues = is_array($queues) ? $queues : [$queues];
            foreach ($queues as $queue) {
                if ($this->declareBindQueue && !in_array($queue, $this->declaredQueues, true)) {
                    $this->channel->queue_declare(
                        $queue,
                        $this->queueParams['passive'],
                        $this->queueParams['durable'],
                        $this->queueParams['exclusive'],
                        $this->queueParams['auto_delete'],
                        false,
                        new AMQPTable($this->queueArguments)
                    );

                    // binding
                    $binding_key = $this->bindingMap[$queue] ?: '';
                    $this->channel->queue_bind($queue, $exchange, $binding_key);

                    $this->declaredQueues[] = $queue;
                }
            }
        }
    }

    /**
     * 使用插件，直接写死部分参数
     *
     * @param $route
     * @param $delay
     */
    private function declareDelayedQueue($route)
    {
        foreach ($route as $exchange => $queues) {
            // declare exchange
            if ($this->declareExchange && !in_array($exchange, $this->declaredExchanges, true)) {
                $this->channel->exchange_declare(
                    $exchange,
                    'x-delayed-message',
                    $this->exchangeParams['passive'],
                    $this->exchangeParams['durable'],
                    false,
                    $this->exchangeParams['auto_delete'],
                    false,
                    new AMQPTable(['x-delayed-type' => AMQPExchangeType::FANOUT])
                );
            }

            // declare delay queue
            $queues = is_array($queues) ? $queues : [$queues];
            foreach ($queues as $queue) {
                if ($this->declareBindQueue && !in_array($queue, $this->declaredQueues, true)) {
                    $this->channel->queue_declare(
                        $queue,
                        $this->queueParams['passive'],
                        $this->queueParams['durable'],
                        $this->queueParams['exclusive'],
                        $this->queueParams['auto_delete'],
                        false,
                        new AMQPTable(['x-dead-letter-exchange' => 'delayed'])
                    );
                    $this->channel->queue_bind($queue, $exchange);

                    $this->declaredQueues[] = $queue;
                }
            }
        }
    }

    public function pushRaw($payload, $routing_key, $exchange = null, array $options = []){
        try {
            $exchange = $this->getExchangeName($exchange);
            $route = $this->getRouteByExchange($exchange);

            if (isset($options['delay']) && $options['delay'] > 0) {
                $this->declareDelayedQueue($route);
            } else {
                $this->declareQueue($route);
            }

            // 发布消息
            $message = $this->ProduceAMQPMessage($payload, $options);
            if ('confirm' == $this->model) {
                $this->publishConfirm($message, $exchange, $routing_key);
            } elseif ('return' == $this->model) {
                $this->publishReturn($message, $exchange, $routing_key);
            } else {
                $this->channel->basic_publish($message, $exchange, $routing_key);
            }

            return $message->has('correlation_id') ? $message->get('correlation_id') : null;
        } catch (\Exception $e) {
            throw $e;
            return null;
        }
    }

    public function publishConfirm($message, $exchange, $routing_key){
        //$this->channel->confirm_select();

        $this->channel->set_ack_handler($this->ack_handler_callback());

        // nack，rabbitMQ内部错误时触发
        $this->channel->set_nack_handler($this->nack_handler_callback($message));

        $this->channel->basic_publish($message, $exchange, $routing_key);

        $this->channel->wait_for_pending_acks();

        return $message;
    }

    public function publishReturn($message, $exchange, $routing_key){
        //$this->channel->confirm_select();

        // 成功到达交换机时执行
        $this->channel->set_ack_handler($this->ack_handler_callback());

        // nack，rabbitMQ内部错误时触发
        $this->channel->set_nack_handler($this->nack_handler_callback($message));

        // 模拟时可以删除queue
        $this->channel->set_return_listener($this->return_listen_callback($message));

        // 设置mandatory=true，监听路由不可达时回调set_return_lister()处理
        $this->channel->basic_publish($message, $exchange, $routing_key, true, false);

        $this->channel->wait_for_pending_acks_returns();

        return $message;
    }

    /**
     * get route by exchange, if not return empty route
     *
     * @param string $exchange
     * @return array
     */
    public function getRouteByExchange($exchange = '')
    {
        $exchange = $this->getExchangeName($exchange);
        return [$exchange => !empty($this->routeMap[$exchange]) ? $this->routeMap[$exchange] : ''];
    }

    public function getRouteByQueue($queue = '')
    {
        $route = [];
        foreach ($this->routeMap as $exchange => $queues) {
            if ((is_array($queues) && in_array($queue, $queues)) || $queue == $queues) {
                $route[$exchange] = $queues;
            }
        }

        return $route;
    }

    /**
     * get exchange
     *
     * @param $exchange
     * @return mixed
     */
    public function getExchangeName($exchange)
    {
        return $exchange ?: $this->defaultExchange;
    }

    /**
     * Retrieves the correlation id, or a unique id.
     *
     * @return string
     */
    public function getCorrelationId()
    {
        return $this->correlationId ?: uniqid('', true);
    }

    /**
     * Sets the correlation id for a message to be published.
     *
     * @param string $id
     *
     * @return void
     */
    public function setCorrelationId($id)
    {
        $this->correlationId = $id;
    }

    /**
     * 获取当前消费标识
     *
     * @param $queue
     * @return string
     */
    public function getConsumerTag($queue)
    {
        return $queue . '_' . getmypid();
    }

    /**
     * set message properties
     *
     * @param $options
     * @return array
     */
    public function setMsgProperties($options){
        $properties = [
            'Content-Type'   => 'application/json',
            'delivery_mode'  => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'correlation_id' => $this->getCorrelationId(),
        ];

        if (isset($options['delay']) && $options['delay'] > 0) {
            $properties['application_headers'] = new AMQPTable(['x-delay' => $options['delay'] * 1000]);
        }

        if ($this->retryAfter !== null) {
            //$properties['application_headers'] = [self::ATTEMPT_COUNT_HEADERS_KEY => ['I', $this->retryAfter]];
        }

        return $properties;
    }

    /**
     * produce AMQPMessage
     *
     * @param $payload
     * @param $options
     * @return AMQPMessage
     */
    public function ProduceAMQPMessage($payload, $options){
        if (is_array($payload) || is_object($payload)) {
            $payload = json_encode($payload);
        }

        return new AMQPMessage($payload, $this->setMsgProperties($options));
    }


    public function ack_handler_callback(){
        return function(AMQPMessage $msg){
            $ack = sprintf('Message[%s] ack, with content:%s, correlation_id:%s' . PHP_EOL,
                $msg->getDeliveryTag(),
                $msg->body,
                $msg->has('correlation_id') ? $msg->get('correlation_id') : '0'
            );
            echo $ack;
            Log::info($ack);
        };
    }

    public function nack_handler_callback($message){
        return function(AMQPMessage $msg) use ($message){
            $message->set('correlation_id', null);

            $nack = sprintf('Message[%s] ack, with content:%s, correlation_id:%s' . PHP_EOL,
                $msg->getDeliveryTag(),
                $msg->body,
                $msg->has('correlation_id') ? $msg->get('correlation_id') : '0'
            );
            echo $nack;
            Log::info($nack);
        };
    }

    public function return_listen_callback($message){
        return function($reply_code, $reply_text, $exchange, $routing_key, AMQPMessage $msg) use ($message){
            $message->set('correlation_id', null);

            $return = sprintf('Message enqueue failed, with content:%s, correlation_id:%s, reply_code:%s, reply_text:%s, exchange:%s, routing_key:%s' . PHP_EOL,
                $msg->getBody(),
                $msg->get('correlation_id'),
                $reply_code,
                $reply_text,
                $exchange,
                $routing_key
            );
            echo $return;
            Log::error($return);
        };
    }
}
