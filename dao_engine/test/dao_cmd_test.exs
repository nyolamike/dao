defmodule DaoCmdTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  # test "can execute a shell command" do
  #   # res = System.cmd("mysql", ["-u root"])
  #   # res = System.shell("mysql -u root -p")
  #   # IO.inspect(res)
  # end

  test "using myxql" do
    # Dao.now()
    context = %{
      "database_type" => "mysql",
      "database_name" => "farmers_db",
      "schema" => %{},
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "track_id" => "4664",
      "reset_db" => true
    }

    query = [
      get: [
        improving_where: [
          farmers: %{
            "name" => "str",
            "dao@where" => {
              {"id", ">", 5},
              "&",
              {"name", "contains", "brendah"}
            }
          }
        ]
      ]
    ]

    results = Dao.execute_real(context, query)
    # IO.inspect(results)
    # context = results["context"]
    # sql =
    #   results["root_cmd_node_list"]
    #   |> Keyword.get(:get)
    #   |> Keyword.get(:improving_where)
    #   |> Keyword.get(:students)
    #   |> Map.get("sql")
    # res = MyXQL.query(:myxql, sql)
    # IO.inspect(res)
  end
end
