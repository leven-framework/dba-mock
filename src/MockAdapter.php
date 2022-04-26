<?php

namespace Leven\DBA\Mock;

use Closure;
use Leven\DBA\Common\AdapterInterface;
use Leven\DBA\Common\AdapterResponse;
use Leven\DBA\Common\Exception\TxnNotActiveException;
use Leven\DBA\Mock\Query\{DeleteQueryBuilder, InsertQueryBuilder, SelectQueryBuilder, UpdateQueryBuilder};
use Leven\DBA\Mock\Structure\Database;

class MockAdapter implements AdapterInterface
{

    protected Database $database;

    protected int $txnDepth = 0;
    protected string $txnRollbackDb;

    public function __construct(
        Database|Closure|array      $database = [],
        protected readonly ?Closure $onUpdate = null,

        public readonly string      $tablePrefix = '',
    )
    {
        if(is_callable($database)) $database = $database();

        $this->database = $database instanceof Database ? $database
            : Database::fromArray($database);
    }

    public function getDatabase(): Database
    {
        return $this->database;
    }

    public function executeQuery(Query $query): AdapterResponse
    {
        if(is_callable($query->update)) ($query->update)($this);

        if(($table = $query->autoIncrementFromTable) !== null)
            $lastId = $this->getDatabase()->getTable($table)->getAutoIncrement();

        return new AdapterResponse($query->count, $query->rows, $lastId ?? null);
    }

    public function save()
    {
        if(is_callable($this->onUpdate) && $this->txnDepth === 0)
            ($this->onUpdate)($this->database);
    }


    // QUERY BUILDERS

    public function select(string $table): SelectQueryBuilder
    {
        return new SelectQueryBuilder($this, $table);
    }

    // alias for select
    public function get(string $table): SelectQueryBuilder
    {
        return $this->select($table);
    }

    public function insert(string $table, ?array $data = null): InsertQueryBuilder|AdapterResponse
    {
        $builder = new InsertQueryBuilder($this, $table);

        if($data === null) return $builder;

        $builder->set($data);
        return $builder->execute();
    }

    public function update(string $table): UpdateQueryBuilder
    {
        return new UpdateQueryBuilder($this, $table);
    }

    public function delete(string $table): DeleteQueryBuilder
    {
        return new DeleteQueryBuilder($this, $table);
    }


    // TRANSACTIONS

    public function txnBegin(): static
    {
        if($this->txnDepth++ === 0)
            $this->txnRollbackDb = serialize($this->database);

        return $this;
    }

    public function txnCommit(): static
    {
        if($this->txnDepth === 0) throw new TxnNotActiveException;
        if(--$this->txnDepth > 0) return $this; // still in txn

        unset($this->txnRollbackDb);
        $this->save();

        return $this;
    }

    public function txnRollback(): static
    {
        if($this->txnDepth === 0) throw new TxnNotActiveException;

        $this->txnDepth = 0;
        $this->database = unserialize($this->txnRollbackDb);
        unset($this->txnRollbackDb);

        return $this;
    }
}
