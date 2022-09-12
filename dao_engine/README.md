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

## Json Config File dao.json

## SQL Resources

* [https://www.youtube.com/watch?v=HXV3zeQKqGY](https://www.youtube.com/watch?v=HXV3zeQKqGY)


on day04 we aslo sitch to using string keys instead of atoms, because the schema part comes from a user input
this can be a potential DOS attack point since atoms are not garbage collected


dao.json

- auto_alter_db_in_production this flag should disable auto altering of db in production environment if db has already been previously created

- accepted_named_queries: this one, in auto create mode will save the name of the query here if its not there
this means that, if someone tries to alter the named queries to by pass restrictions, those unknown queries wont run

- an api config, allows converting named queries into REST API calls

- "schema_timestamps": true, - by default these are turned on, but if one does not want to have these accross all tables it can be set to false
however any local  "dao@timestamps": false, will overrride this


- WHen data is being inserted as an array without specifying columns into a table one needs to follow the alphabetic order of the columns

*** right now we need to add the ability for all sqls in a named query to be tracked

** can it work starting with an existing db and them auto generating a schema from it and then use that

** now given a query like the one below 
query = [
      add: [
        employees_table: [
          employeexx: %{
            "emp_id" => "pk",
            "first_name" => "str 40",
            "last_name" => "str 40",
            "birth_day" => "date",
            "sex" => "str 1",
            "sex" => "int",
            "super_id" => "int",
            "branch_id" => "int"
          }
        ]
      ]
    ]
  in the context of add, if all columns are definitions then it will auto infer "dao@def_only" => true
  there is no need to have that as a flag



> Quering Interface (DSL), using data structures and flags
> Generating SQL from Queries
> Executing queries 
> Modifiying Context
> Packaging and Returning response


Road Ahead Not in Any Order
> Documentation
> Tutorilas
> Client UI libraries/ Language Libraries
> Query processing tracing - for debugging while bulding this tool
> Error handling and reporting
> Working starting with an existing DB
> Multitenant
> Queues and Pipelines
> Triggers
> Hooks
> Migrations
> Transactions and Rollbacks
> Prepared Statements
> Security and Authorization
> Email
> SMS
> Payments
> Notifications
> Audit Trails and Logs
> Hashing
> SQL Views
> Encryption
> Hidden/Invisible Fields
> Realtime Capabilities
> If conditions
> Server side functions
> Server side rendering
> Static Variable Pages (Sort of how Next.js, Gatsby, Static Site Generator)
> Concurrency
> File uploads/ Downloads
> Batch processing using excell, scv
> QR code generations capabilities
> Image manipulation
> Plugins
> Cache State
> Event Bus/ Queue e.g RabitmQ
> SUpport for Postgress, MSSQL. Oracle

