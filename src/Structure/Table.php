<?php

namespace Leven\DBA\Mock\Structure;

use Leven\DBA\Common\Exception\DriverException;

class Table
{

    protected bool $hasAutoIncrementColumn = false;

    /** @var Column[] $columns */
    protected array $columns = [];

    protected array $rows = [];


    public function __construct(
        public readonly string $name,
        protected ?int         $autoIncrement = null,
    )
    {
    }


    // GETTERS

    public function getAutoIncrement(): ?int
    {
        return $this->autoIncrement;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function getColumnNames(): array
    {
        return array_keys($this->getColumns());
    }

    public function getColumnIndex(string $columnName): int
    {
        if (($key = array_search($columnName, $this->getColumnNames())) === false)
            throw new DriverException("column `$columnName` does not exist in table `$this->name`");

        return $key;
    }

    public function getRows(): array
    {
        return $this->rows;
    }


    // ARRAY CONVERTERS

    public static function fromArray(string $key, array $value): self
    {
        $table = new static(
            name: $key,
            autoIncrement: $value['autoIncrement'] ?? null,
        );

        $table->columnsFromArray($value[0]);

        foreach ($value as $index => $row)
            if ($index !== 0 && $index !== 'autoIncrement')
                $table->addRow($row, true);

        return $table;
    }

    protected function columnsFromArray(array $array): void
    {
        foreach ($array as $columnName => $columnProps)
            // allow to pass column object directly as value
            $this->addColumn(($columnProps instanceof Column) ? $columnProps
                : Column::fromArray($columnName, $columnProps));
    }

    public function toArray(): array
    {
        return [
            'autoIncrement' => $this->autoIncrement,
            $this->columnsToArray(),
            ...$this->getRows()
        ];
    }

    protected function columnsToArray(): array
    {
        foreach ($this->columns as $column)
            $output[$column->name] = $column->toArray();

        return $output ?? [];
    }


    // MANIPULATION METHODS

    public function addColumn(Column $column): void
    {
        if($column->autoIncrement) {
            if($this->hasAutoIncrementColumn)
                throw new DriverException("table `$this->name` cannot have more than one autoIncrement column");

            $this->hasAutoIncrementColumn = true;

            // if it's not null that means it has been configured from table array
            if($this->autoIncrement === null) $this->autoIncrement = 0;
        }

        $this->columns[$column->name] = $column;
    }

    protected function validateRow(array $row, ?int $skipUniqueCheckForRowIndex = null): void
    {
        foreach($row as $fieldIndex => $field) {
            $columnName = $this->getColumnNames()[$fieldIndex];
            $column = $this->columns[$columnName];

            $column->validateValue($field);

            if($column->unique) $uniqueChecks[$fieldIndex] = $field;
        }

        if (empty($uniqueChecks)) return; // do we need to perform are any unique checks

        foreach ($this->getRows() as $checkingRowIndex => $checkingRow)
            if($checkingRowIndex !== $skipUniqueCheckForRowIndex) // skip current row if same value is being set again
                foreach ($uniqueChecks as $fieldIndex => $field)
                    if ($checkingRow[$fieldIndex] === $field)
                        throw new DriverException("attempted to add non unique value in table `$this->name`");
    }

    protected function getDefaultRow(bool $noAutoIncrement = false): array
    {
        foreach($this->getColumns() as $column)
            $output[] = $column->autoIncrement && !$noAutoIncrement
                ? ++$this->autoIncrement
                : $column->default
            ;

        return $output ?? [];
    }

    /**
     * @param bool $noAutoIncrement if true, autoIncrement value for table will not be incremented,
     *                              used when converting table from array
     *
     * @return int|null autoIncrement value of the added row or null of there is no autoIncrement column
     */
    public function addRow(array $row, bool $noAutoIncrement = false): ?int
    {
        $row += $this->getDefaultRow($noAutoIncrement);
        ksort($row); // array + merging messes up the order key order for some reason

        $this->validateRow($row);
        $this->rows[] = $row;

        return $this->autoIncrement;
    }

    public function mergeIntoRow(int $index, array $row): void
    {
        $this->validateRow($row, $index); // validate only new fields
        // second param is to skip unique check if existing unique values are being overwritten with same values

        $row += $this->rows[$index];
        ksort($row); // array + merging messes up the order key order for some reason

        $this->rows[$index] = $row;
    }


    public function deleteColumn(string ...$columnNames): void
    {
        foreach($columnNames as $columnName) {
            $columnIndex = $this->getColumnIndex($columnName);

            // delete column
            unset($this->columns[$columnName]);

            // delete each field for this column
            foreach($this->rows as &$row) {
                $row = array_values($row); // reset indices because now one column is missing
                unset($row[$columnIndex]);
            }
        }
    }

    public function deleteRow(int ...$indexes): void
    {
        foreach($indexes as $index)
            unset($this->rows[$index]);
    }

    public function uasortRows(callable $callback): void
    {
        uasort($this->rows, $callback);
    }

}