<?php

namespace Dev\MySQL\Transaction;

use Dev\MySQL\Exception\ConnectException;
use Dev\MySQL\Exception\TransactionException;
use Dev\MySQL\Pool\IPool;
use Dev\MySQL\Connector\IConnector;
use Psr\Log\LoggerInterface;

/**
 * 协程版事务管理器
 * 注意：事务开启直到提交/回滚的过程中会一直占用某个 IConnector 实例，如果有很多长事务，则会很快耗完连接池资源
 * Class Transaction
 * @package Dev\MySQL\Transaction
 */
class CoTransaction implements ITransaction
{
    private $pool;
    private $context;
    private $logger;

    public function __construct(IPool $pool, LoggerInterface $logger = null)
    {
        $this->pool = $pool;
        $this->logger = $logger;
        $this->context = new TContext();
    }

    /**
     * @throws \Exception
     */
    public function __destruct()
    {
        // 如果事务没有结束，则回滚
        if ($this->isRunning()) {
            $this->rollback();
        }
    }

    /**
     * @param string $model write/read 读模式还是写模式，针对读写分离
     * @param bool $isImplicit 是否隐式事务，隐式事务不会向 MySQL 提交 begin 请求
     * @return bool
     * @throws \Exception
     */
    public function begin(string $model = 'write', bool $isImplicit = false): bool
    {
        // 如果事务已经开启了，则直接返回
        if ($this->isRunning()) {
            return true;
        }

        // 事务模式
        $this->model($model);
        $this->isRunning(true);

        // 获取 Connector
        try {
            if (!($connector = $this->connector())) {
                throw new ConnectException("获取连接失败");
            }
        } catch (\Exception $exception) {
            $this->isRunning(false);
            throw new TransactionException($exception->getMessage(), $exception->getCode());
        }

        $this->resetLastExecInfo();
        $this->clearSQL();

        if ($this->logger) {
            $this->logger->info("transaction begin");
        }

        return $isImplicit || $connector->begin();
    }

    /**
     * 发送指令
     * @param string $preSql
     * @param array $params
     * @return bool|mixed
     * @throws
     */
    public function command(string $preSql, array $params = [])
    {
        if (!$preSql) {
            return false;
        }

        // 是否隐式事务：外界没有调用 begin 而是直接调用 command 则为隐式事务
        $isImplicit = !$this->isRunning();

        // 如果是隐式事务，则需要自动开启事务
        if ($isImplicit && !$this->begin($this->calcModelFromSQL($preSql), true)) {
            return false;
        }

        $result = $this->exec([$preSql, $params]);

        // 隐式事务需要及时提交
        if ($isImplicit && !$this->commit($isImplicit)) {
            return false;
        }

        if ($this->logger) {
            $this->logger->info("[SQL]:{$preSql},params:" . print_r($params, true));
        }

        return $result;
    }

    /**
     * 提交事务
     * @param bool $isImplicit 是否隐式事务，隐式事务不会向 MySQL 提交 commit
     * @return bool
     * @throws \Exception
     */
    public function commit(bool $isImplicit = false): bool
    {
        if (!$this->isRunning()) {
            return true;
        }

        $result = true;
        if (!$isImplicit) {
            if ($conn = $this->connector(false)) {
                $result = $conn->commit();
                if ($result === false) {
                    // 执行失败，试图回滚
                    $this->rollback();
                    return false;
                }
            } else {
                return false;
            }
        }

        // 释放事务占用的资源
        $this->releaseTransResource();

        return $result;
    }

    /**
     * @return bool
     * @throws \Exception
     */
    public function rollback(): bool
    {
        if (!$this->isRunning()) {
            return true;
        }

        if ($conn = $this->connector(false)) {
            $conn->rollback();
            if ($this->logger) {
                $this->logger->info("transaction rollback");
            }
        }

        $this->releaseTransResource();
        return true;
    }

    /**
     * 获取或设置当前事务执行模式
     * @param string$model read/write
     * @return string
     */
    public function model(?string $model = null): string
    {
        // 事务处于开启状态时不允许切换运行模式
        if (!isset($model) || $this->isRunning()) {
            return $this->context['model'];
        }

        $this->context['model'] = $model === 'read' ? 'read' : 'write';

        return $model;
    }

    public function lastInsertId()
    {
        return $this->getLastExecInfo('insert_id');
    }

    public function affectedRows()
    {
        return $this->getLastExecInfo('affected_rows');
    }

    public function lastError()
    {
        return $this->getLastExecInfo('error');
    }

    public function lastErrorNo()
    {
        return $this->getLastExecInfo('error_no');
    }

    public function sql(): array
    {
        return $this->context['sql'] ?? [];
    }

    /**
     * 释放当前协程的事务资源
     * @throws \Exception
     */
    private function releaseTransResource()
    {
        // 保存本次事务相关执行结果
        $this->saveLastExecInfo();
        // 归还连接资源
        $this->giveBackConnector();

        unset($this->context['model']);

        $this->isRunning(false);
    }

    /**
     * @throws \Exception
     */
    private function saveLastExecInfo()
    {
        if ($conn = $this->connector(false)) {
            $this->context['last_exec_info'] = [
                'insert_id' => $conn->insertId(),
                'error' => $conn->lastError(),
                'error_no' => $conn->lastErrorNo(),
                'affected_rows' => $conn->affectedRows(),
            ];
        } else {
            $this->context['last_exec_info'] = [];
        }
    }

    private function resetLastExecInfo()
    {
        unset($this->context['last_exec_info']);
    }

    private function getLastExecInfo(string $key)
    {
        return isset($this->context['last_exec_info']) ? $this->context['last_exec_info'][$key] : '';
    }

    /**
     * 执行指令池中的指令
     * @param $sqlInfo
     * @return mixed
     * @throws
     */
    private function exec(array $sqlInfo)
    {
        if (!$sqlInfo || !$this->isRunning()) {
            return true;
        }

        return $this->connector()->query($sqlInfo[0], $sqlInfo[1]);
    }

    private function clearSQL()
    {
        unset($this->context['sql']);
    }

    private function calcModelFromSQL(string $sql): string
    {
        if (preg_match('/^(update|replace|delete|insert|drop|grant|truncate|alter|create)\s/i', trim($sql))) {
            return 'write';
        }

        return 'read';
    }

    /**
     * 获取连接资源
     * @param bool $usePool
     * @return IConnector
     * @throws \Dev\MySQL\Exception\ConnectException
     * @throws \Dev\MySQL\Exception\ConnectFatalException
     * @throws \Dev\MySQL\Exception\PoolClosedException
     */
    private function connector(bool $usePool = true)
    {
        if ($connector = $this->context['connector']) {
            return $connector;
        }

        if (!$usePool) {
            return null;
        }

        $this->context['connector'] = $this->pool->getConnector($this->model());

        return $this->context['connector'];
    }

    /**
     * 归还连接资源
     */
    private function giveBackConnector()
    {
        if ($this->context['connector']) {
            $this->pool->pushConnector($this->context['connector']);
        }

        unset($this->context['connector']);
    }

    private function isRunning(?bool $val = null)
    {
        if (isset($val)) {
            $this->context['is_running'] = $val;
        } else {
            return $this->context['is_running'] ?? false;
        }
    }
}