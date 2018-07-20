<?php
/**
 * Copyright (c) 2018,上海二三四五网络科技股份有限公司
 * 文件名称：RedisQueue.php
 * 摘    要：RedisQueue分装Queue
 * 作    者：Fangz
 * 修改日期：2018.07.19
 */

namespace jackeryuna\queue\queues;

use Octopus\RedisEx;
use jackeryuna\queue\base\Queue;
use jackeryuna\queue\helper\ArrayHelper;
use jackeryuna\queue\base\Object;

class RedisQueue extends Queue
{
    /**
     * RedisEx连接实例
     */
    public $connector;

    public function __construct( $redisConf )
    {
        echo 1111;
        exit;
        $this->init($redisConf);
    }

    public function init( $redisConf )
    {
        parent::init();

        if ( !class_exists('RedisEx') ) {
            throw new \Exception('the extension RedisEx does not exist ,you need it to operate RedisEx');
        }

        if ( !$this->connector instanceof RedisEx ) {
            $this->connector = RedisEx::getInstance("redis", $redisConf['default']);
        }
    }

    /**
     * 入队列
     * @param $job
     * @param string $data
     * @param null $queue
     * @return int
     */
    protected function push( $job, $data = '', $queue = null )
    {
        return $this->connector->rpush($this->getQueue($queue), $this->createPayload($job, $data, $queue));
    }

    /**
     * 出队列
     * @param null $queue
     * @return object
     * @throws \yii\base\InvalidConfigException
     */
    public function pop( $queue = null )
    {
        $original = $queue ?: $this->queue;
        $queue = $this->getQueue($queue);

        $job = $this->connector->lpop($queue);

        if ( !is_null($job) ) {

            $config =  [
                'class' => 'Octopus\queue\jobs\RedisJob',
                'queue' => $original,
                'job' => $job,
                'queueInstance' => $this,
            ];

            return Object::createObject($config);
        }

        return false;
    }

    /**
     * 清空指定队列
     * @param null $queue
     * @return integer
     * @throws \Exception execution failed
     */
    public function flush($queue = null)
    {
        $queue = $this->getQueue($queue);
        return $this->connector->del([$queue, $queue . ":delayed", $queue . ":reserved"]);

    }

    /**
     * 给队列数据添加id和attempts字段
     * @param  string $job
     * @param  mixed $data
     * @param  string $queue
     * @return string
     */
    protected function createPayload($job, $data = '', $queue = null)
    {
        $payload = parent::createPayload($job, $data);
        $payload = $this->setMeta($payload, 'id', $this->getRandomId());
        return $this->setMeta($payload, 'attempts', 1);
    }

    /**
     * 创建一个随机串作为id
     * @param int $length
     * @return string
     */
    protected function getRandomId()
    {
        $string = md5(time() . rand(1000, 9999));
        return $string;
    }

    /**
     * 获取队列名称（即redis里面的key）
     * @param  string|null $queue
     * @return string
     */
    protected function getQueue($queue)
    {
        return 'queues:' . ($queue ?: $this->queue);
    }

    /**
     * 在输入数据中添加新的字段
     * @param  string $payload
     * @param  string $key
     * @param  string $value
     * @return string
     */
    protected function setMeta($payload, $key, $value)
    {
        $payload = unserialize($payload);
        $newPayload = serialize(ArrayHelper::set($payload, $key, $value));
        return $newPayload;
    }

    /**
     * 获取队列当前任务数 = 执行队列任务数 + 等待队列任务数
     * @param null $queue
     * @return mixed
     */
    public function getJobCount($queue = null)
    {
        $queue = $this->getQueue($queue);
        return $this->connector->llen($queue) + $this->connector->zcard($queue . ":delayed");
    }

    /**
     * 从已处理集合中删除一个任务
     * @param  string $queue
     * @param  string $job
     * @return void
     */
    public function deleteReserved($queue, $job)
    {
        $this->connector->zrem($this->getQueue($queue) . ':reserved', $job);
    }
}