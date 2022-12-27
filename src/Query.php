<?php

namespace Dev\MySQL;

use Dev\MySQL\Exception\DBException;
use Dev\MySQL\Transaction\ITransaction;
use Dev\MySQL\Transaction\TContext;

/**
 * 查询器，对外暴露的 API
 * Class Query
 * @package Dev\MySQL
 */
class Query
{
    use Builder;

    public const MODEL_READ = 'read';
    public const MODEL_WRITE = 'write';

    private $transaction;

    /**
     * Query constructor.
     * @param ITransaction $transaction 事务管理器
     */
    public function __construct(ITransaction $transaction)
    {
        $this->transaction = $transaction;
        $this->context = new TContext();
    }

    /**
     * 开启事务
     * @param string $model
     * @return bool
     * @throws \Exception
     */
    public function begin($model = 'write'): bool
    {
        return $this->transaction->begin($model);
    }

    /**
     * 提交事务
     * @return bool
     * @throws \Exception
     */
    public function commit(): bool
    {
        return $this->transaction->commit();
    }

    /**
     * 回滚事务
     * @return bool
     * @throws \Exception
     */
    public function rollback(): bool
    {
        return $this->transaction->rollback();
    }

    /**
     * 强制设置使用读库还是写库
     * @param string $model read/write
     * @return string
     * @throws \Exception
     */
    public function setModel(string $model)
    {
        if (!in_array($model, [self::MODEL_READ, self::MODEL_WRITE])) {
            throw new \Exception("非法的 model 标识：{$model}。仅支持 read/write");
        }
        $this->transaction->model($model);
        return $this;
    }

    /**
     * 便捷方法：列表查询
     * @return array
     * @throws \Exception
     */
    public function list(): array
    {
        $list = $this->transaction->command(...$this->compile());
        if ($list === false) {
            throw new DBException($this->lastError(), $this->lastErrorNo());
        }

        return $list;
    }

    /**
     * 便捷方法：查询一行记录
     * @return array|false
     * @throws \Exception
     */
    public function one(): array
    {
        $list = $this->transaction->command(...$this->limit(1)->compile());

        if ($list === false) {
            throw new DBException($this->lastError(), $this->lastErrorNo());
        }

        if ($list) {
            return $list[0];
        }

        return [];
    }

    /**
     * 便捷方法：查询某个字段的值
     * @return mixed
     * @throws DBException
     */
    public function column()
    {
        $res = $this->transaction->command(...$this->compile());

        if ($res === false) {
            throw new DBException($this->lastError(), $this->lastErrorNo());
        }

        return $res ? reset($res[0]) : '';
    }

    /**
     * 便捷方法：分页查询
     * 注意：page 中执行了两次 command，这之间会发生协程切换，而查询器是多携程共享的，所以必须正确处理数据隔离
     * @return array|false
     * @throws DBException
     */
    public function page(): array
    {
        $this->limit = $this->limit ?: 20;
        $this->offset = $this->offset ?: 0;

        // compile 之前先暂存属性值，供后面使用
        $this->stash();
        // 暂存 model
        $model = $this->transaction->model();
        $countRes = $this->transaction->command(...$this->fields('count(*) as cnt')->reset('limit')->compile());
        if ($countRes === false) {
            $this->stashClear();
            throw new DBException($this->lastError(), $this->lastErrorNo());
        }

        if (!$countRes || !$countRes[0]['cnt']) {
            $this->stashClear();
            return ['total' => 0, 'data' => []];
        }

        $this->stashApply();
        // 将 model 和前面设置一致
        $this->transaction->model($model);
        $data = $this->transaction->command(...$this->compile());

        if ($data === false) {
            throw new DBException($this->lastError(), $this->lastErrorNo());
        }

        return ['total' => $countRes[0]['cnt'], 'data' => $data];
    }

    /**
     * 执行 SQL
     * 有两种方式：
     *  1. 调此方法时传入相关参数；
     *  2. 通过 Builder 提供的 Active Record 方法组装 SQL，调此方法（不传参数）执行并返回结果
     * @param string $preSql
     * @param array $params
     * @return int|array 影响的行数|数据集
     * @throws \Exception
     */
    public function execute(string $preSql = '', array $params = [])
    {
        if (!func_num_args()) {
            $result =  $this->transaction->command(...$this->compile());
        } else {
            $result = $this->transaction->command(...$this->prepareSQL($preSql, $params));
        }

        if ($result === false) {
            throw new DBException($this->lastError() . '.raw sql:' . $this->rawSql()[0], $this->lastErrorNo());
        }

        return $result;
    }

    /**
     * @return int
     */
    public function lastInsertId()
    {
        return $this->transaction->lastInsertId();
    }

    /**
     * @return string
     */
    public function lastError()
    {
        return $this->transaction->lastError() ?: '';
    }

    /**
     * @return int
     */
    public function lastErrorNo()
    {
        return intval($this->transaction->lastErrorNo() ?: 500);
    }

    public function affectedRows()
    {
        return $this->transaction->affectedRows();
    }

    /**
     * 将查询器的属性值暂存到当前协程上下文中，防止在协程切换中被破坏（或污染其他协程）
     */
    private function stash()
    {
        $data = [];
        foreach ($this as $propKey => $propVal) {
            if (in_array($propKey, ['transaction', 'context'])) {
                continue;
            }
            $data[$propKey] = $propVal;
        }

        $this->context['stash'] = $data;
    }

    /**
     * 将当前协程暂存区的数据应用到查询器上
     */
    private function stashApply()
    {
        if (!isset($this->context['stash'])) {
            return;
        }

        $data = $this->context['stash'];
        unset($this->context['stash']);

        foreach ($this as $propKey => $_) {
            if (in_array($propKey, ['transaction', 'context'])) {
                continue;
            }
            $this->{$propKey} = $data[$propKey] ?? null;
        }
    }

    /**
     * 清除暂存区数据
     */
    private function stashClear()
    {
        unset($this->context['stash']);
    }
}
