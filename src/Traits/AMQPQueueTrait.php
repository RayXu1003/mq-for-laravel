<?php

namespace RuiXu\MessageQueue\Traits;

use ErrorException;
use Exception;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Exception\AMQPException;
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
            if ($this->declareExchange && !in_array($exchange, $this->declaredExchanges, true)) {
                // declare exchange
                $this->channel->exchange_declare(
                    $exchange,
                    $this->exchangeParams['type'],
                    $this->exchangeParams['passive'],
                    $this->exchangeParams['durable'],
                    $this->exchangeParams['auto_delete']
                );

                $this->declaredExchanges[] = $exchange;
            }

            $queues = is_array($queues) ? $queues : [$queues];
            foreach ($queues as $queue) {
                if ($this->declareBindQueue && !in_array($queue, $this->declaredQueues, true)) {
                    // declare queue
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

    private function declareDelayedQueue($route, $delay)
    {
        $routeMap = [];
        foreach ($route as $exchange => $queues)
        {
            $delay = $this->secondsUntil($delay);
            $exchange = $this->getExchangeName($exchange);
            $delay_queue = $exchange . '_deferred_' . $delay;

            // declare exchange
            if ($this->declareExchange && !in_array($exchange, $this->declaredExchanges, true)) {
                $this->channel->exchange_declare(
                    $exchange,
                    $this->exchangeParams['type'],
                    $this->exchangeParams['passive'],
                    $this->exchangeParams['durable'],
                    $this->exchangeParams['auto_delete']
                );
            }

            $queues = is_array($queues) ? $queues : [$queues];
            foreach ($queues as $queue)
            {
                // declare exchange
                if ($this->declareBindQueue && !in_array($queue, $this->declaredQueues, true)) {
                    // declare queue
                    $this->channel->queue_declare(
                        $queue,
                        $this->queueParams['passive'],
                        $this->queueParams['durable'],
                        $this->queueParams['exclusive'],
                        $this->queueParams['auto_delete'],
                        false,
                        new AMQPTable($this->queueArguments)
                    );
                    $this->channel->queue_bind($queue, $exchange, '');
                    $this->declaredQueues[] = $queue;
                }
            }

            // declare  delay queue
            if (!in_array($delay_queue, $this->declaredQueues, true)) {
                $queueArguments = array_merge([
                    'x-dead-letter-exchange' => $exchange,
                    'x-dead-letter-routing-key' => '',
                    'x-message-ttl' => $delay * 1000,
                ], (array)$this->queueArguments);

                $this->channel->queue_declare(
                    $delay_queue,
                    $this->queueParams['passive'],
                    $this->queueParams['durable'],
                    $this->queueParams['exclusive'],
                    $this->queueParams['auto_delete'],
                    false,
                    new AMQPTable($queueArguments)
                );
                $this->channel->queue_bind($delay_queue, $exchange, $delay_queue);
                $this->declaredQueues[] = $delay_queue;
            }

            // bind queue to the exchange

            $routeMap[$exchange] = $delay_queue;
        }
        return $routeMap;
    }

    public function pushRaw($payload, $routing_key, $exchange = null, array $options = []){
        try {
            $exchange = $this->getExchangeName($exchange);
            $route = $this->getRouteByExchange($exchange);
            if (isset($options['delay']) && $options['delay'] > 0) {
                $this->declareDelayedQueue($route, $options['delay']);
            } else {
                $this->declareQueue($route);
            }

            // 发布消息
            $message = $this->ProduceAMQPMessage($payload);
            if ('confirm' == $this->model) {
                $this->publishConfirm($message, $exchange, $routing_key);
            } elseif ('return' == $this->model) {
                $this->publishReturn($message, $exchange, $routing_key);
            } else {
                $this->channel->basic_publish($message, $exchange, $routing_key);
            }

            return $message->has('correlation_id') ? $message->get('correlation_id') : null;
        } catch (ErrorException $e) {
            Log::error('[ErrorException] ' . $e->getMessage());
            throw $e;
            return null;
        } catch (AMQPRuntimeException $e) {
            Log::error('[RuntimeException] ' . $e->getMessage());
            throw $e;
            return null;
        } catch (AMQPTimeoutException $e) {
            Log::error('[TimeoutException] ' . $e->getMessage());
            throw $e;
            return null;
        } catch (AMQPException $e) {
            Log::error('[AMQPException] ' . $e->getMessage());
            throw $e;
            return null;
        } catch (\Exception $e) {
            Log::error('[Exception] ' . $e->getMessage());
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
     * 设置消息属性
     *
     * @return array
     */
    public function setMsgProperties(){
        $properties = [
            'Content-Type'   => 'application/json',
            'delivery_mode'  => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'correlation_id' => $this->getCorrelationId(),
        ];

        if ($this->retryAfter !== null) {
            $properties['application_headers'] = [self::ATTEMPT_COUNT_HEADERS_KEY => ['I', $this->retryAfter]];
        }

        return $properties;
    }

    /**
     * produce AMQPMessage
     *
     * @param $payload
     * @return AMQPMessage
     */
    public function ProduceAMQPMessage($payload){
        if (is_array($payload) || is_object($payload)) {
            $payload = json_encode($payload);
        }

        return new AMQPMessage($payload, $this->setMsgProperties());
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
