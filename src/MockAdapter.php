<?php namespace Leven\DBA\Mock;

// NOT WORKING:
//  - $options['offset']
//  - $options['order']
//  - transactions

use Leven\DBA\Common\{DatabaseAdapterInterface, DatabaseAdapterResponse};
use Leven\DBA\Common\Exception\{
    ArgumentValidationException,
    EmptyResultException,
    Driver\DriverException,
    Driver\NotImplementedException
};
use Leven\DBA\Mock\Exception\{MockTableAlreadyExistsException, MockTableNotFoundException};
use Closure;

final class MockAdapter implements DatabaseAdapterInterface
{

    public function __construct(
        private array $schema,
        private array $store,
        private ?Closure $saveData = null
    )
    {
    }


    public function escapeValue(string $string): string
    {
        return $string;
    }

    public function escapeName(string $string): string
    {
        return $string;
    }


    public function schema(string $table): array
    {
        if (!isset($this->schema[$table]))
            throw new DriverException(previous: new MockTableNotFoundException);

        return $this->schema[$table];
    }


    /**
     * @throws DriverException
     */
    public function createTable(string $table, array $schema = []): MockAdapter
    {
        if (isset($this->schema[$table]))
            throw new DriverException(previous: new MockTableAlreadyExistsException());

        $this->schema[$table] = $schema;
        $this->store[$table] = [];

        if(is_callable($this->saveData)) ($this->saveData)($this->schema, $this->store);

        return $this;
    }

    /**
     * @throws DriverException
     */
    public function modifyTable(string $table, array $instructions = [])
    {
        if (!isset($this->schema[$table]))
            throw new DriverException(previous: new MockTableNotFoundException);

        // TODO
    }

    /**
     * @throws DriverException
     */
    public function count(string $table): int
    {
        if (!isset($this->schema[$table]))
            throw new DriverException(previous: new MockTableNotFoundException);

        return count($this->store[$table] ?? []);
    }



    public function get(string $table, array|string $columns = '*', array $conditions = [], array $options = []): DatabaseAdapterResponse
    {
        if (!isset($this->schema[$table]))
            throw new DriverException(previous: new MockTableNotFoundException);

        if (!isset($this->store[$table])) $this->store[$table] = [];

        if (is_string($columns) && $columns != '*') $columns = [$columns];

        $rows = [];
        $indexes = $this->filterRowsByConditions($table, $conditions, $options);
        foreach ($indexes as $index) {

            $rows[] = $this->store[$table][$index];

            // delete all columns that weren't asked for
            if ($columns != '*')
                foreach ($this->store[$table][$index] as $column => $value)
                    if (!in_array($column, $columns)) unset($rows[count($rows) - 1][$column]);
        }

        $response = new DatabaseAdapterResponse(
            count: count($rows),
            rows: $rows
        );

        if (($options['single'] ?? false)) {
            if (count($rows) === 0) throw new EmptyResultException;
            $response->row = array_values($rows)[0];
        }

        return $response;
    }

    public function insert(string $table, array $data): DatabaseAdapterResponse
    {
        if (!isset($this->schema[$table]))
            throw new DriverException(previous: new MockTableNotFoundException);

        if (!isset($this->store[$table])) $this->store[$table] = [];

        if (is_array($data[0] ?? false)) {
            foreach ($data as $row) {
                $this->validate($table, $row);
            }

            $this->store[$table] = array_merge($this->store[$table], $data);
            $count = count($data);
        } else {
            $this->store[$table][] = $this->validate($table, $data);
            $count = 1;
        }

        if(is_callable($this->saveData)) ($this->saveData)($this->schema, $this->store);

        return new DatabaseAdapterResponse(
            count: $count
        );
    }

    public function update(string $table, array $data, array $conditions = [], array $options = []): DatabaseAdapterResponse
    {
        if (!isset($this->schema[$table]))
            throw new DriverException(previous: new MockTableNotFoundException);

        if(!isset($this->store[$table])) $this->store[$table] = [];

        $indexes = $this->filterRowsByConditions($table, $conditions, $options);
        foreach ($indexes as $index)
            foreach ($this->validate($table, $data) as $col => $val)
                $this->store[$table][$index][$col] = $val;

        if(is_callable($this->saveData)) ($this->saveData)($this->schema, $this->store);

        return new DatabaseAdapterResponse(
            count: count($indexes)
        );
    }

    public function delete(string $table, array $conditions = [], array $options = []): DatabaseAdapterResponse
    {
        if (!isset($this->schema[$table]))
            throw new DriverException(previous: new MockTableNotFoundException);

        if(!isset($this->store[$table])) $this->store[$table] = [];

        $indexes = $this->filterRowsByConditions($table, $conditions, $options);
        foreach ($indexes as $index)
            unset($this->store[$table][$index]);

        if(is_callable($this->saveData)) ($this->saveData)($this->schema, $this->store);

        return new DatabaseAdapterResponse(
            count: count($indexes)
        );
    }

    /**
     * @throws NotImplementedException
     */
    public function txnBegin()
    {
        throw new NotImplementedException;
    }

    /**
     * @throws NotImplementedException
     */
    public function txnCommit()
    {
        throw new NotImplementedException;
    }

    /**
     * @throws NotImplementedException
     */
    public function txnRollback()
    {
        throw new NotImplementedException;
    }

    // INTERNAL METHODS

    /**
     * @throws ArgumentValidationException
     */
    private function validate(string $table, array $row): array
    {
        $schema = $this->schema[$table];

        foreach ($row as $column => $value) {
            if (!isset($schema[$column]))
                throw new ArgumentValidationException("column $column does not exist in table $table");

            if(!is_null($value) && !is_bool($value) && !is_numeric($value) && !is_string($value))
                throw new ArgumentValidationException("column $column in $table is neither null/bool/string/number, can't be stored in database");

            $schemaForColumn = strtolower($schema[$column]);

            if((str_contains($schemaForColumn, 'not null')) && $value === null)
                throw new ArgumentValidationException("column $column in $table may not be null");

            $type = explode('(', $schemaForColumn);
            if (isset($type[1])) {
                if (strlen($value) > rtrim($type[1], ')'))
                    throw new ArgumentValidationException("value in column $column exceeds column's max length");
            }

            if ($type[0] === 'any') continue; // allow any value

            else if (in_array($type[0], ['varchar', 'text']) && !is_string($value))
                throw new ArgumentValidationException("value of column $column is not string");

            else if($type[0] === 'json'){
                $result = json_decode($value); if (json_last_error() !== 0)
                    throw new ArgumentValidationException("value of column $column is not valid json");
            }

            else if ($type[0] === 'int' && !is_int($value))
                throw new ArgumentValidationException("value of column $column is not int");

            else if ($type[0] === 'float' && !is_float($value) && !is_int($value))
                throw new ArgumentValidationException("value of column $column is not float");
        }

        return $row;
    }

    private function filterRowsByConditions(string $table, array $conditions, array $options = []): array
    {
        if ($options['single'] ?? false) $options['limit'] = 1;
        $count = 0;
        $indexes = [];

        foreach ($this->store[$table] as $index => $row) {
            // eliminate all rows that don't fit conditions
            foreach ($conditions as $cond_col => $cond_val)
                if (!isset($row[$cond_col]) || $row[$cond_col] != $cond_val) continue 2;

            // conditions passed, add index to return list
            $indexes[] = $index;

            // if limit is set and we already have enough rows
            if (($options['limit'] ?? 0) != 0 && ++$count >= $options['limit']) break;
        }

        return $indexes;
    }
}
