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
    public function test_redis_lists_insert_remove()
    {
        $this->lists->insert('whitelist', 1, 10);
        $this->lists->insert('whitelist', 2, 1);
        $all = $this->lists->all('whitelist');
        $this->assertTrue(count($all) === 2);
        sleep(2);
        $all = $this->lists->all('whitelist');
        $this->assertTrue(count($all) === 1);
        $this->lists->insert('whitelist', 1, 1);
        $this->lists->insert('whitelist', 3, 10);
        sleep(2);
        $all = $this->lists->all('whitelist');
        $this->assertTrue($all === ['3']);
        $this->lists->remove('whitelist', 3);
        $this->lists->insert('whitelist', 4, 10);
        $all = $this->lists->all('whitelist');
        $this->assertTrue($all === ['4']);
    }

    /**
     * @test
     */
    public function test_redis_lists_take()
    {
        $this->lists->insert('whitelist', 1, 10);
        $this->lists->insert('whitelist', 2, 10);
        $this->lists->insert('whitelist', 3, 10);
        $taken = [
            $this->lists->take('whitelist', 1),
            $this->lists->take('whitelist', 1),
            $_ = $this->lists->take('whitelist', 10),
        ];
        sort($taken);
        $this->assertTrue($taken === ['1', '2', '3']);
        $taken = [
            $this->lists->take('whitelist', 1),
            $this->lists->take('whitelist', 1),
        ];
        sort($taken);
        $this->assertTrue(!in_array($_, $taken));
    }

    /**
     * @test
     */
    public function test_redis_lists_virtual()
    {
        $this->lists->insert('whitelist', 1, 10);
        $this->lists->insert('whitelist', 2, 10);
        $this->lists->insert('whitelist', 3, 10);
        $taken = [
            $this->lists->take('whitelist:1', 10),
            $this->lists->take('whitelist:1', 10),
            $_ = $this->lists->take('whitelist:1', 10),
        ];
        sort($taken);
        $this->assertTrue($taken === ['1', '2', '3']);
        $taken = [
            $this->lists->take('whitelist:2', 10),
            $this->lists->take('whitelist:2', 10),
            $_ = $this->lists->take('whitelist:2', 10),
        ];
        sort($taken);
        $this->assertTrue($taken === ['1', '2', '3']);
    }

    /**
     * @test
     */
    public function test_redis_lists_expire()
    {
        $this->lists->insert('whitelist', 1, 1000);
        $this->lists->insert('whitelist', 2, 1000);
        $this->lists->insert('whitelist', 3, 1000);
        $mc = microtime(1);
        foreach (range(1, 10) as $i) {
            $this->lists->take('whitelist', 0);
        }
        $mc = round(microtime(1) - $mc, 1);
        $this->assertTrue($mc < 1);
    }
}
