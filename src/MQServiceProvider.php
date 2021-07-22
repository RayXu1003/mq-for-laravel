<?php

namespace Rex\MessageQueue;

use Illuminate\Support\ServiceProvider;

class MQServiceProvider extends ServiceProvider
{
    protected static $driver = 'amqp';

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        $queues_config = config('mq.connections');
        $queues_config = $queues_config ?? [];
        foreach ($queues_config as $key => $config) {
            if ($config['driver'] == self::$driver) {
                $this->mergeConfigFrom(
                    __DIR__ . '/config/amqp.php', 'mq.connections.' . $key
                );
            }
        }
    }

    /**
     * Register the application's event listeners.
     *
     * @return void
     */
    public function boot()
    {
        $this->app->singleton('mq', function ($app) {
            return tap(new MessageQueueManager($app), function($manager){
                $manager->addConnector(self::$driver, function(){
                    return new Connectors\AMQPConnector;
                });
            });
        });
    }
}
