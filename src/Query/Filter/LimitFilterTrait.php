<?php

namespace Leven\DBA\Mock\Query\Filter;

use Leven\DBA\Common\BuilderPart\LimitTrait;
use Leven\DBA\Mock\Structure\Table;

trait LimitFilterTrait
{

    use LimitTrait;

    protected function filterLimit(Table $table): Table
    {
        if($this->limit === 0) return $table;

        $count = 0;
        foreach($table->getRows() as $index => $value) {
            if($count < $this->offset || $count >= $this->limit + $this->offset)
                $table->deleteRow($index);

            $count++;
        }

        return $table;
    }

}