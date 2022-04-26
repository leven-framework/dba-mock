<?php

namespace Leven\DBA\Mock\Query;

use Leven\DBA\Common\AdapterResponse;
use Leven\DBA\Mock\MockAdapter;
use Leven\DBA\Mock\Query;
use Leven\DBA\Mock\Structure\Table;

abstract class BaseQueryBuilder
{

    // parameter order is reverse compared to MySQL adapter because here adapter is required
    public function __construct(
        public MockAdapter $adapter,
        public string      $table,
    )
    {
    }

    abstract public function getQuery(): Query;

    final public function execute(): AdapterResponse
    {
        return $this->adapter->executeQuery($this->getQuery());
    }

    final protected function pipe(mixed $initialValue, callable ...$callables): mixed
    {
        return array_reduce($callables, fn($carry, $item) => $item($carry), $initialValue);
    }


    final protected function getTableCopy(): Table
    {
        $table = $this->adapter->tablePrefix . $this->table;

        // shallow copy is enough because we're only going to be modifying table rows (array)
        // if I ever implement column aliases, we will need to deep copy
        return clone $this->adapter->getDatabase()->getTable($table);
    }

    final protected function getRowIndices(Table $table): array
    {
        foreach($table->getRows() as $index => $row)
            $output[] = $index;

        return $output ?? [];
    }

}