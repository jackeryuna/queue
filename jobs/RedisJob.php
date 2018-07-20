<?php
/**
 * Redis job处理
 * User: fangz fangz@2345.com
 * Date: 2018/07/20
 * Time: 08:57
 */
namespace Octopus\queue\jobs;

use Octopus\queue\base\Job;
use Octopus\queue\helper\ArrayHelper;

class RedisJob extends Job
{

    public function getAttempts()
    {
        return ArrayHelper::get(unserialize($this->job), 'attempts');
    }

    public function getPayload()
    {
        return $this->job;
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return ArrayHelper::get(unserialize($this->job), 'id');
    }

    /**
     * Delete the job from the queue.
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();
        $this->queueInstance->deleteReserved($this->queue, $this->job);
    }

    /**
     * Release the job back into the queue.
     *
     * @param  int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        parent::release($delay);
        $this->delete();
        $this->queueInstance->release($this->queue, $this->job, $delay, $this->getAttempts() + 1);
    }
}