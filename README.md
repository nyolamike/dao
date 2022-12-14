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
* Internally the query is converted into an sql statement
```sql
SELECT school_app.students.name as mdmd FROM `school_app.students` LEFT JOIN kkffl to gkgk WHERE `school_app.students.name` like '%mike%'
```
* The query is executed and the results are formated according to the structure of the query
```
case results do
  {:ok, data } -> IO.inspect( data )
  {:error, errors } -> IO.inspect( errors )
end
```
* The data inspected above will be like this
```elixir
%{
  students: [
    %{
      id: 1,
      name: "mike nyola",
      class_room_id: 5,
      class_room: %{
        id: 5,
        name: "grade 6"
      }
    },
    %{
      id: 5,
      name: "mike tyler",
      class_room_id: 5,
      class_room: %{
        id: 5,
        name: "grade 6"
      }
    }
  ]
}
```



