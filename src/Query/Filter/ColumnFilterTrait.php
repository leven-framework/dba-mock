<?php

namespace Leven\DBA\Mock\Query\Filter;

use Leven\DBA\Common\BuilderPart\ColumnTrait;
use Leven\DBA\Mock\Structure\Table;

trait ColumnFilterTrait
{

    use ColumnTrait;

    protected function filterColumn(Table $table): Table
    {
        if(is_string($this->columns) || empty($this->columns)) return $table;

        // iterate through all column names
        foreach($table->getColumnNames() as $index => $column){
            if(in_array($column, $this->columns)) continue; // column is in keep list

            $table->deleteColumn($column);
        }

        return $table;
    }

}