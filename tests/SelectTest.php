<?php

namespace Leven\DBA\Mock\Tests;

use Leven\DBA\Common\BuilderPart\WhereGroup;
use Leven\DBA\Mock\MockAdapter;

$db = new MockAdapter([
    'empty_table' => [ ['bar' => ['string'], 'baz' => ['string']] ],
    'basic' => [['bar' => ['string'], 'baz' => ['string'], 'number' => ['int']],
        ['first', 'one', 1],
        ['second', 'two', 2],
    ],
    'complex' => [ ['id' => ['int'], 'name' => ['string'], 'active' => ['bool'], 'age' => ['int']],
        [1, 'Alice', true, 25],
        [2, 'Bob', false, 27],
        [3, 'Chris', true, 23],
        [4, 'Dean', true, 24],
        [5, 'Eva', false, 27],
        [6, 'Floria', false, 22],
        [7, 'Gina', true, 25],
        [8, 'Helen', false, 22],
        [9, 'Ivan', true, 25],
        [10, 'John', false, 29],

    ],
]);

test('empty table select', function () use ($db) {
    $result = $db->select('empty_table')->execute();

    expect($result->rows)->toBeEmpty()
        ->and($result->count)->toBe(0);
});

test('basic select all', function () use ($db) {
    $result = $db->select('basic')->execute();

    expect($result->rows)->toBe(
        [ ['bar' => 'first', 'baz' => 'one', 'number' => 1], ['bar' => 'second', 'baz' => 'two', 'number' => 2] ]
    )->and($result->count)->toBe(2);
});

test('basic select all with one column', function () use ($db) {
    $result = $db->select('basic')->columns('baz')->execute();

    expect($result->rows)->toBe([ ['baz' => 'one'], ['baz' => 'two'] ])
        ->and($result->count)->toBe(2);

    $result = $db->select('basic')->columns('number')->execute();

    expect($result->rows)->toBe([ ['number' => 1], ['number' => 2] ])
        ->and($result->count)->toBe(2);
});

test('complex select', function () use ($db) {
    $result = $db->select('complex')
        ->where('active', true)
        ->orderDesc('id')
        ->limit(1)->offset(1)
        ->execute();

    expect($result->rows)->toBe([ ['id' => 7, 'name' => 'Gina', 'active' => true, 'age' => 25] ])
        ->and($result->count)->toBe(1);

    $result = $db->select('complex')
        ->where('active', false)
        ->orderAsc('age')->orderDesc('id')
        ->limit(2)->offset(1)
        ->execute();

    expect($result->rows)->toBe([
        ['id' => 6, 'name' => 'Floria', 'active' => false, 'age' => 22],
        ['id' => 5, 'name' => 'Eva', 'active' => false, 'age' => 27],
    ])->and($result->count)->toBe(2);

    $result = $db->select('complex')
        ->where(fn (WhereGroup $w) => $w
            ->where('age', '<=', 22)
            ->orWhere('age', '>', 28)
        )
        ->andWhere('name', '!=', 'Helen')
        ->orderDesc('name')
        ->limit(4)
        ->columns('name', 'age')
        ->execute();

    expect($result->rows)->toBe([
        ['name' => 'John', 'age' => 29],
        ['name' => 'Floria', 'age' => 22],
    ])->and($result->count)->toBe(2);
});