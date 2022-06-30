<?php

namespace Leven\DBA\Mock\Query\Filter;

use Le\SMPLang\SMPLang;
use Leven\DBA\Common\BuilderPart\{WhereCondition, WhereGroup, WhereTrait};
use Leven\DBA\Mock\Structure\Table;

trait WhereFilterTrait
{

    use WhereTrait;

    protected static function genWhereExpression(array $conditions): string
    {
        $exp = '';
        if(empty($conditions)) return $exp;

        foreach ($conditions as $condition) {
            if(!empty($exp)) $exp .= $condition->isOr ? ' || ' : ' && ';

            if($condition instanceof WhereGroup) {
                $exp .= static::genWhereExpression($condition->getConditions());
            } else
            if($condition instanceof WhereCondition) {
                $value = $condition->value;
                if(is_string($value)) $value = json_encode($value); // quote and escape string
                else if(is_bool($value)) $value = $value ? 'true' : 'false'; // convert boolean to string
                else if(is_null($value)) $value = 'null'; // convert null to string

                $operand = $condition->operand;
                if($operand === '<=>' || $operand === '=') $operand = '=='; // achieve mysql-like behavior

                $exp .= "$condition->column $operand $value";
            }
        }

        return empty($exp) ? $exp : "($exp)";
    }

    protected function filterWhere(Table $table): Table
    {
        $exp = static::genWhereExpression($this->conditions);
        if($exp === '') return $table;

        $el = new SMPLang;
        foreach($table->getRows() as $index => $row){
            $result = $el->evaluate($exp, array_combine($table->getColumnNames(), $row));
            if($result === false) $table->deleteRow($index);
        }

        return $table;
    }

}