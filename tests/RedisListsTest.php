<?php

use Ejz\RedisClient;
use Ejz\RedisLists;
use PHPUnit\Framework\TestCase;

class RedisListsTest extends TestCase
{
    /** @var RedisLists */
    private $lists;

    /**
     *
     */
    public function setUp()
    {
        parent::setUp();
        $redisClient = new RedisClient();
        $redisClient->FLUSHDB();
        $this->lists = new RedisLists($redisClient);
    }

    /**
     * @test
     */
    public function test_redis_lists_simple()
    {
        $l = $this->lists;
    }
}
