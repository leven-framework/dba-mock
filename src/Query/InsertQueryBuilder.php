<?php

namespace Leven\DBA\Mock\Query;

use Leven\DBA\Common\InsertQueryInterface;
use Leven\DBA\Mock\MockAdapter;
use Leven\DBA\Mock\Query;
use Leven\DBA\Mock\Query\Filter\SetFilterTrait;

class InsertQueryBuilder extends BaseQueryBuilder implements InsertQueryInterface
{

    use SetFilterTrait;

    public function getQuery(): Query
    {
        $update = function(MockAdapter $adapter) {
            $table = $adapter->getDatabase()->getTable($this->table);

            $row = $this->formatSetDataToRow($table);
            $table->addRow($row);

            $adapter->save();
        };

        return new Query(1, [], $update, $this->adapter->tablePrefix . $this->table);
    }

}