# DaoEngine

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `dao_engine` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:dao_engine, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/dao_engine>.

## SELECT

```elixir
%{
  get: {
    list_of: %{
      shops: %{}
    },
  }
}
```

should be translated as

``` elixir
SELECT * FROM `TN.shops` WHERE `TN.shops.is_deleted == false`
```

should return

```elixir
%{
  shops: [%{}...]
}
```

## Config

* auto_create

  If a table or column does not exist, it is auto created, before the query is executed
The dao.json file is updated after db changes have been executed
If the query gets an error also a rollback of these atcions must be executed incase a config of rollback schema auto create schema changes has been set to true

  These queries must be executed in a transaction manner, either they all succeed or
the whole request fails due to fatal error

* parrallel

* When a name of a table cannot be plural or single,
one can add a config of is_list to either indicate that the intent is for a list
or false to indicate a single item, by default the engine assumes a list

* schema
The schema is kept in a json file called dao.json and controlled by a single process
Requests to modify/alter the schema during runtime are allowed by setting the
auto_alter_db config setting to true, by default this is false.

  When a table doesnot exist in the db and is part of a request and auto_alter_db is set
to true, then that table must be created before other queries are executed, infact
during the process of converting a query to sql, these scenarios are identified and
scheduled for execution

* create database

Before running any sqls in the kwl_nodes or root nodes make sure to reverse the list before execution
because during the process of generating the sql statements these lists are appended at the start for
perfomrace reasons.

* feature -> Decribe table_name; to return table schema from the actual db, some modifications to get the schema from the dao way.

* feature -> Drop table_name; to delete a table from the db and also schema

* feature -> Alter Table table_name ADD column_name datatype;

* feature -> Alter Table table_name DROP COLUMN column_name; removing a primary key table should remove all foreign keys
if a config flag of propagate_pk_changes_on_alter

## Data Types

* done: boolean
* done: string
* done: pk
* DECIMAL(3, 2) -> where 3 is the total digits and 2 the number of decimal places



## SQL Resources

* [https://www.youtube.com/watch?v=HXV3zeQKqGY](https://www.youtube.com/watch?v=HXV3zeQKqGY)
