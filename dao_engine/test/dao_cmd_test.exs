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
      "database_name" => "grocerify",
      "schema" => %{},
      "auto_schema_changes" => [],
      "auto_alter_db" => false,
      "track_id" => "4664"
    }

    query = [
      get: [
        improving_where: [
          students: %{
            "dao@where" => {
              {"id", ">", 5},
              "&",
              {"name", "contains", "brendah"}
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    # IO.inspect(results["root_cmd_node_list"])
  end
end
