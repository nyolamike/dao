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

* parrallel

- when a name of a table cannot be plural or single, one can add a config of is_list to either
indicate that the intent is for a list or false to indicate a single item, by default the engine
assumes a list
