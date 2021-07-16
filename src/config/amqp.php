<?php

/**
 * This is an example of queue connection configuration.
 * It will be merged into config/mq.php.
 * You need to set proper values in `.env`
 */
return [

    'driver'   => 'amqp',

    'host'     => env('AMQP_HOST', '127.0.0.1'),
    'port'     => env('AMQP_PORT', 5672),
    'login'    => env('AMQP_LOGIN', 'guest'),
    'password' => env('AMQP_PASSWORD', 'guest'),
    'vhost'    => env('AMQP_VHOST', '/'),

    /*
     * The name of default queue.
     */
    'default_queue'    => env('AMQP_QUEUE'),
    'default_exchange' => env('AMQP_EXCHANGE'),

    /*
     * exchange - queue maps
     */
    'route' => [],

    /*
     * Determine if exchange should be created if it does not exist.
     */
    'exchange_declare' => env('AMQP_EXCHANGE_DECLARE', false),

    /*
     * Determine if queue should be created and binded to the exchange if it does not exist.
     */
    'queue_declare_bind' => env('AMQP_QUEUE_DECLARE_BIND', false),

    /*
     * Read more about possible values at https://www.rabbitmq.com/tutorials/amqp-concepts.html
     */
    'queue_params' => [
        'passive'     => env('AMQP_QUEUE_PASSIVE', false),
        'durable'     => env('AMQP_QUEUE_DURABLE', true),
        'exclusive'   => env('AMQP_QUEUE_EXCLUSIVE', false),
        'auto_delete' => env('AMQP_QUEUE_AUTODELETE', false),
        'arguments'   => env('AMQP_QUEUE_ARGUMENTS'),
    ],
    'exchange_params' => [
        'type'        => env('AMQP_EXCHANGE_TYPE', 'direct'),
        'passive'     => env('AMQP_EXCHANGE_PASSIVE', false),
        'durable'     => env('AMQP_EXCHANGE_DURABLE', true),
        'auto_delete' => env('AMQP_EXCHANGE_AUTODELETE', false),
    ],

    /*
     * Determine the number of seconds to sleep if there's an error communicating with rabbitmq
     * If set to false, it'll throw an exception rather than doing the sleep for X seconds.
     */
    'sleep_on_error' => env('AMQP_ERROR_SLEEP', 5),

    /*
     * Optional SSL params if an SSL connection is used
     */
    'ssl_params' => [
        'ssl_on'      => env('AMQP_SSL', false),
        'cafile'      => env('AMQP_SSL_CAFILE', null),
        'local_cert'  => env('AMQP_SSL_LOCALCERT', null),
        'verify_peer' => env('AMQP_SSL_VERIFY_PEER', true),
        'passphrase'  => env('AMQP_SSL_PASSPHRASE', null),
    ],

    // debug model
    'debug' => env('APP_DEBUG', false),
];
