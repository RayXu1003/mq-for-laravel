<?php

namespace RuiXu\MessageQueue\Traits;

use ErrorException;
use Exception;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Message\AMQPMessage;

trait AMQPQueueTrait {

    protected $channel;

    protected $declareExchange;
    protected $declareBindQueue;
    protected $sleepOnError;

    protected $queueParameters;
    protected $queueArguments;
    protected $configExchange;

    private $declaredExchanges = [];
    private $declaredQueues = [];

    private $retryAfter = null;
    private $correlationId = null;

    public function size($queue = null)
    {
        list(, $messageCount) = $this->channel->queue_declare($this->getQueueName($queue), true);

        return $messageCount;
    }

    public function getChannel()
    {
        return $this->connection->channel();
    }

    private function declareQueue($route)
    {
        $routeMap = [];
        foreach ($route as $exchange => $queues)
        {
            if ($this->declareExchange && !in_array($exchange, $this->declaredExchanges, true)) {
                // declare exchange
                $this->channel->exchange_declare(
                    $exchange,
                    $this->configExchange['type'],
                    $this->configExchange['passive'],
                    $this->configExchange['durable'],
                    $this->configExchange['auto_delete']
                );

                $this->declaredExchanges[] = $exchange;
            }

            $queues = is_array($queues) ? $queues : [$queues];
            foreach ($queues as $queue)
            {
                if ($this->declareBindQueue && !in_array($queue, $this->declaredQueues, true)) {
                    // declare queue
                    $this->channel->queue_declare(
                        $queue,
                        $this->queueParameters['passive'],
                        $this->queueParameters['durable'],
                        $this->queueParameters['exclusive'],
                        $this->queueParameters['auto_delete'],
                        false,
                        new AMQPTable($this->queueArguments)
                    );
                    $this->declaredQueues[] = $queue;
                    $this->channel->queue_bind($queue, $exchange, '');
                }
            }

            if (is_array($queues)) {
                $routeMap[$exchange] = reset($queues);
            } else {
                $routeMap[$exchange] = $queues;
            }

        }

        return $routeMap;
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
                    $this->configExchange['type'],
                    $this->configExchange['passive'],
                    $this->configExchange['durable'],
                    $this->configExchange['auto_delete']
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
                        $this->queueParameters['passive'],
                        $this->queueParameters['durable'],
                        $this->queueParameters['exclusive'],
                        $this->queueParameters['auto_delete'],
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
                    $this->queueParameters['passive'],
                    $this->queueParameters['durable'],
                    $this->queueParameters['exclusive'],
                    $this->queueParameters['auto_delete'],
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

    public function pushRaw($payload, $exchange = null, array $options = [])
    {
        try {
            if (is_array($payload) || is_object($payload)) {
                $payload = json_encode($payload);
            }
            $exchange = $this->getExchangeName($exchange);
            $route = $this->getRouteByExchange($exchange);
            if (isset($options['delay']) && $options['delay'] > 0) {
                $routeMap = $this->declareDelayedQueue($route, $options['delay']);
            } else {
                $routeMap = $this->declareQueue($route);
            }

            $headers = [
                'Content-Type' => 'application/json',
                'delivery_mode' => 2,
            ];

            if ($this->retryAfter !== null) {
                $headers['application_headers'] = [self::ATTEMPT_COUNT_HEADERS_KEY => ['I', $this->retryAfter]];
            }

            // push job to a queue
            $message = new AMQPMessage($payload, $headers);

            $correlationId = $this->getCorrelationId();
            $message->set('correlation_id', $correlationId);

            // push task to a queue
            $this->channel->basic_publish($message, $exchange, !empty($routeMap[$exchange]) ? $routeMap[$exchange] : '');

            return $correlationId;
        } catch (ErrorException $exception) {
            throw $exception;
            return null;
        }
    }

    public function getExchangeByQueue($queue='')
    {
        if (empty($queue) || $this->defaultQueue==$queue) {
            return $this->defaultExchange;
        }
        return !empty($this->routeMap[$queue]) ? $this->routeMap[$queue] : null;
    }

    public function getRouteByExchange($exchange = '')
    {
        $exchange = $this->getExchangeName($exchange);
        return [$exchange => !empty($this->routeMap[$exchange]) ? $this->routeMap[$exchange] : ''];
    }

    public function getRouteByQueue($queue = '')
    {
        $route = [];
        foreach ($this->routeMap as $exchange => $queues)
        {
            if (
                (is_array($queues) && in_array($queue, $queues)) ||
                $queue == $queues
            ) {
                $route[$exchange] = $queues;
            }
        }
        return $route;
    }

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
     * @author leesenlen
     */
    public function getConsumerTag()
    {
        return config('app.name').'_'.getmypid();
    }
}
