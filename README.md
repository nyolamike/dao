# philosophy
Dao is the natural order of the universe whose character one's intuition must discern to realize the potential for individual wisdom, as conceived in the context of East Asian philosophy, East Asian religions, or any other philosophy or religion that aligns to this principle. 
(Wikipedia)[https://en.wikipedia.org/wiki/Tao]

## Data Access Object - dao
Dao is a recreation of the B.E.E (back end enginee) in elixir.

The idea is to compose organic queries that self describe the reponse structure of the data from a database

## How it works
* Compose a query using maps
```elixir
query = %{
  nector: {}
  students: %{
    class_room: %{},
    _w: { :name, :contains_ignore_case,  "mike" }
  }
}
```
* Pass the query to a dao function
```elixir
results = Dao.get(query)
```
