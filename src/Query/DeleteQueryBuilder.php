<?php

namespace Leven\DBA\Mock\Query;

use Leven\DBA\Common\DeleteQueryInterface;
use Leven\DBA\Mock\MockAdapter;
use Leven\DBA\Mock\Query;
use Leven\DBA\Mock\Query\Filter\{LimitFilterTrait, OrderFilterTrait, WhereFilterTrait};

class DeleteQueryBuilder extends BaseQueryBuilder implements DeleteQueryInterface
{

    use WhereFilterTrait;
    use LimitFilterTrait;
    use OrderFilterTrait;

    public function getQuery(): Query
    {
        $indices = $this->pipe(
            $this->getTableCopy(),
            $this->filterWhere(...),
            $this->filterOrder(...),
            $this->filterLimit(...),
            $this->getRowIndices(...),
        );

        $update = function(MockAdapter $adapter) use ($indices) {
            $table = $adapter->getDatabase()->getTable($this->table);
            $table->deleteRow(...$indices);
            $adapter->save();
        };

        return new Query(count($indices), [], $update);
    }

}