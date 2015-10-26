<?php
namespace ElasticsearchORM;
use Psr\Log\AbstractLogger;

class ProfilerLogger extends AbstractLogger
{
    protected static $_instance;
    protected  $messageQueue=[];
    protected $closed=false;
    public function close()
    {
        $this->closed=true;
    }
    /**
     * Logs with an arbitrary level.
     *
     * @param mixed $level
     * @param string $message
     * @param array $context
     * @return null
     */
    public function log($level, $message, array $context = array())
    {
        $this->pushMessageQueue($message, $context);
    }
    public function pushMessageQueue($message,array $context)
    {
        if(!$this->closed){
            $this->messageQueue[]=[$message,$context];
        }
    }
    /**
     * @return array
     */
    public function getMessages()
    {
        return $this->messageQueue;
    }
    /**
     * @return ProfilerLogger
     */
    public static function getInstance()
    {
        if(static::$_instance == null){
            static::$_instance=new ProfilerLogger();
        }
        return static::$_instance;
    }
}