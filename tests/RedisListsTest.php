<?php

use Ejz\RedisLists;
use Ejz\RedisClient;
use PHPUnit\Framework\TestCase;

class RedisListsTest extends TestCase
{
    /** @var RedisLists */
    private $lists;

    /**
     * @return void
     */
    public function setUp(): void
    {
        parent::setUp();
        $client = new RedisClient();
        $client->FLUSHDB();
        $this->lists = new RedisLists($client);
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
        $this->assertTrue($all === [3]);
        $this->lists->remove('whitelist', 3);
        $this->lists->insert('whitelist', 4, 10);
        $all = $this->lists->all('whitelist');
        $this->assertTrue($all === [4]);
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
        $this->assertTrue($taken === [1, 2, 3]);
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
            $this->lists->take('whitelist:1', 10),
        ];
        sort($taken);
        $this->assertTrue($taken === [1, 2, 3]);
        $taken = [
            $this->lists->take('whitelist:2', 10),
            $this->lists->take('whitelist:2', 10),
            $this->lists->take('whitelist:2', 10),
        ];
        sort($taken);
        $this->assertTrue($taken === [1, 2, 3]);
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

    /**
     * @test
     */
    public function test_redis_lists_lru()
    {
        $this->lists->insert('tt', 1, 1000);
        $this->lists->insert('tt', 2, 1001);
        $this->lists->insert('tt', 3, 1002);
        $this->lists->insert('tt', 4, 1);
        $this->lists->insert('tt', 44, 2);
        $this->lists->insert('tt', 5, 1003);
        $this->assertEquals(4, $this->lists->lru('tt'));
        sleep(2);
        $this->assertEquals(1, $this->lists->lru('tt'));
    }

    /**
     * @test
     */
    public function test_redis_lists_pack_unpack()
    {
        $items = [1, '1', 1.1, [1], null, new stdClass()];
        foreach ($items as $item) {
            $this->lists->insert('tt', $item, 1000);
        }
        foreach ($items as $_) {
            $item = $this->lists->take('tt', 100);
            $this->assertTrue(
                in_array(serialize($item), array_map('serialize', $items))
            );
        }
    }
}
